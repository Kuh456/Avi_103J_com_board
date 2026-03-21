#![no_std]
#![no_main]
#![deny(
    clippy::mem_forget,
    reason = "mem::forget is generally not safe to do with esp_hal types, especially those \
    holding buffers for the duration of a data transfer."
)]
#![deny(clippy::large_stack_frames)]
use C99Jcomboard::{gnss_setting, parse_gga, parse_rmc_movement};
use core::fmt::Write;
#[allow(
    clippy::large_stack_frames,
    reason = "it's not unusual to allocate larger buffers etc. in main"
)]
use embassy_executor::Spawner;
use embassy_executor::raw;
use embassy_futures::select::{Either, Either3, select, select3};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex, channel::Channel, mutex::Mutex, signal::Signal,
};
use embassy_time::{Delay, Duration, Timer};
use embedded_can::{Frame, Id};
use embedded_hal_bus::spi::ExclusiveDevice;
use embedded_sdmmc::{SdCard, TimeSource, Timestamp, VolumeIdx, VolumeManager};
use heapless::String;
struct SdTimeSource {
    timer: Rtc<'static>,
}

impl SdTimeSource {
    fn new(timer: Rtc<'static>) -> Self {
        Self { timer }
    }
    fn current_time(&self) -> u64 {
        self.timer.current_time_us()
    }
}
static TZ: jiff::tz::TimeZone = jiff::tz::get!("UTC");
impl TimeSource for SdTimeSource {
    fn get_timestamp(&self) -> Timestamp {
        let now_us = self.current_time();
        // Convert to jiff Time
        let now = jiff::Timestamp::from_microsecond(now_us as i64).unwrap();
        let now = now.to_zoned(TZ.clone());
        Timestamp {
            year_since_1970: (now.year() - 1970).unsigned_abs() as u8,
            zero_indexed_month: now.month().wrapping_sub(1) as u8,
            zero_indexed_day: now.day().wrapping_sub(1) as u8,
            hours: now.hour() as u8,
            minutes: now.minute() as u8,
            seconds: now.second() as u8,
        }
    }
}

use core::sync::atomic::{AtomicBool, Ordering};
use embassy_time::Instant;
use esp_backtrace as _;
use esp_hal::spi::{self, master::Spi};
use esp_hal::time::Rate;
use esp_hal::{
    Async,
    clock::CpuClock,
    gpio::{Input, InputConfig, Level, Output, OutputConfig},
    interrupt::software::SoftwareInterruptControl,
    rtc_cntl::Rtc,
    system::Stack,
    timer::timg::TimerGroup,
    twai::{self, BaudRate, EspTwaiFrame, StandardId, TwaiMode, filter::SingleStandardFilter},
    uart::{Config as UartConfig, DataBits, Parity, StopBits, Uart, UartRx},
};
use esp_println::println;
use esp_rtos::embassy::Executor;
use static_cell::StaticCell;
// 各ノードからの最終受信時刻
static LAST_SEEN_LOG: Mutex<CriticalSectionRawMutex, Option<Instant>> = Mutex::new(None);
static LAST_SEEN_CAMERA: Mutex<CriticalSectionRawMutex, Option<Instant>> = Mutex::new(None);
static LAST_SEEN_POWER: Mutex<CriticalSectionRawMutex, Option<Instant>> = Mutex::new(None);

type GnssPacket = [u8; 90];
static TRIGGER_SIGNAL: Signal<CriticalSectionRawMutex, bool> = Signal::new();
static GNSS_CHANNEL: Channel<CriticalSectionRawMutex, GnssPacket, 5> = Channel::new();
static RMC_CHANNEL: Channel<CriticalSectionRawMutex, [f32; 2], 5> = Channel::new();
static RECEIVED_DATA_CHANNEL: Channel<CriticalSectionRawMutex, u8, 10> = Channel::new();
static PAYLOAD_MUTEX: Mutex<CriticalSectionRawMutex, Payload> = Mutex::new(Payload::new());
static RAW_GNSS_CHANNEL: Channel<CriticalSectionRawMutex, GnssPacket, 10> = Channel::new();
static IS_LOGGING: AtomicBool = AtomicBool::new(false);
static HAS_UNFLUSHED_DATA: AtomicBool = AtomicBool::new(false);
static CAN_TX_CHANNEL: Channel<CriticalSectionRawMutex, (u16, u8), 5> = Channel::new();
static IS_CAN_ERROR: AtomicBool = AtomicBool::new(true);
pub static GNSS_CMD_CHANNEL: Channel<CriticalSectionRawMutex, GnssCommand, 2> = Channel::new();

const CAN_ID_EMERGENCY_STOP_PARA: u16 = 0x003;
const CAN_ID_STOP_SEQUENCE: u16 = 0x00a;
const CAN_ID_START_SEQUENCE: u16 = 0x005;
const CAN_ID_ACTUATE_PARA: u16 = 0x00d;
const CAN_ID_REVERSE_PARA: u16 = 0x00f;
const CAN_ID_STAET_LOGGING: u16 = 0x011;
const CAN_ID_STOP_LOGGING: u16 = 0x01e;
const CAN_ID_STAET_RECORDING: u16 = 0x020;
const CAN_ID_STOP_RECORDING: u16 = 0x02a;
const CAN_ID_POWER_ON_CAMERA: u16 = 0x02f;
const CAN_ID_POWER_OFF_CAMERA: u16 = 0x030;
const CAN_ID_ANGLE_SPEED: u16 = 0x120;
const CAN_ID_ACCELARATION: u16 = 0x11a;
const CAN_ID_AIR_PRESSURE: u16 = 0x10a;
const CAN_ID_LIFT_OFF: u16 = 0x110;
const CAN_ID_TOP: u16 = 0x12a;
const CAN_ID_CAMERA_STATUS: u16 = 0x200;
const CAN_ID_TEST_TO_LOG_PARA: u16 = 0x300;
const CAN_ID_TEST_FROM_LOG_PARA: u16 = 0x301;
const CAN_ID_TEST_TO_CAMERA: u16 = 0x310;
const CAN_ID_TEST_FROM_CAMERA: u16 = 0x311;
const CAN_ID_TEST_TO_POWER_CONTROL: u16 = 0x320;
const CAN_ID_TEST_FROM_POWER_CONTROL: u16 = 0x321;
const BUF_SIZE: usize = 8192;

#[derive(Debug, Copy, Clone)]
struct Payload {
    add_h: u8,
    add_l: u8,
    chnnl: u8,
    header1: u8,
    header2: u8,
    status: u8,             // 1 byte
    gnss_lat: i32,          // 4 byte
    gnss_long: i32,         // 4 byte
    angle_speed: [i16; 3],  // 2 byte * 3 = 6 byte
    acceleration: [i16; 3], // 2 byte * 3 = 6 byte
    air_pressure: i16,      // 2 byte
    check_sum: u8,          // 1 byte
}
impl Payload {
    const fn new() -> Self {
        Self {
            add_h: 0x00,
            add_l: 0x00,
            chnnl: 0x03,
            header1: 0xaa,
            header2: 0x55,
            status: 0,
            gnss_lat: 0,
            gnss_long: 0,
            angle_speed: [0; 3],
            acceleration: [0; 3],
            air_pressure: 0,
            check_sum: 0,
        }
    }
    /// 自身のデータ（ヘッダーから気圧まで）のチェックサムを計算する
    fn calculate_checksum(&self) -> u8 {
        let size = core::mem::size_of::<Self>();

        // 構造体自身をバイトの配列(&[u8])」として強制的に読み替える
        let bytes: &[u8] = &self.to_bytes();
        // let bytes: &[u8] =
        //     unsafe { core::slice::from_raw_parts(self as *const Self as *const u8, size) };

        let mut sum: u8 = 0;
        // 最初の3バイトと最後の1バイト（check_sum自身）を除いた部分
        for &byte in &bytes[3..size - 1] {
            sum ^= byte;
        }
        sum
    }
    /// ペイロードを安全にバイト配列に変換する
    pub fn to_bytes(&self) -> [u8; 33] {
        let mut buf = [0u8; 33];
        let mut offset = 0;

        buf[offset] = self.add_h;
        offset += 1;
        buf[offset] = self.add_l;
        offset += 1;
        buf[offset] = self.chnnl;
        offset += 1;
        buf[offset] = self.header1;
        offset += 1;
        buf[offset] = self.header2;
        offset += 1;
        buf[offset] = self.status;
        offset += 1;

        // i32 -> 4bytes (リトルエンディアン)
        buf[offset..offset + 4].copy_from_slice(&self.gnss_lat.to_le_bytes());
        offset += 4;
        buf[offset..offset + 4].copy_from_slice(&self.gnss_long.to_le_bytes());
        offset += 4;

        // [i16; 3] -> 6bytes
        for &val in &self.angle_speed {
            buf[offset..offset + 2].copy_from_slice(&val.to_le_bytes());
            offset += 2;
        }
        // [i16; 3] -> 6bytes
        for &val in &self.acceleration {
            buf[offset..offset + 2].copy_from_slice(&val.to_le_bytes());
            offset += 2;
        }

        // i16 -> 2bytes
        buf[offset..offset + 2].copy_from_slice(&self.air_pressure.to_le_bytes());
        offset += 2;

        buf[offset] = self.check_sum;

        buf
    }

    /// チェックサムを計算し、check_sum フィールドに代入する
    fn update_checksum(&mut self) {
        self.check_sum = self.calculate_checksum();
    }
}
// GNSSの電源ON/OFFを指示するコマンド
#[derive(Debug, Clone, Copy)]
enum GnssCommand {
    TurnOn,
    TurnOff,
}

esp_bootloader_esp_idf::esp_app_desc!();
fn create_can_frame_to_send(can_id: u16, cmd: u8) -> EspTwaiFrame {
    EspTwaiFrame::new(StandardId::new(can_id).unwrap(), &[cmd]).unwrap()
}
const fn get_target_can_id(cmd: u8) -> Option<u16> {
    match cmd {
        b'e' => Some(CAN_ID_STOP_SEQUENCE),
        b's' => Some(CAN_ID_START_SEQUENCE),
        b'p' => Some(CAN_ID_ACTUATE_PARA),
        b'r' => Some(CAN_ID_REVERSE_PARA),
        b'l' => Some(CAN_ID_STAET_LOGGING),
        b'm' => Some(CAN_ID_STOP_LOGGING),
        b'c' => Some(CAN_ID_STAET_RECORDING),
        b'v' => Some(CAN_ID_STOP_RECORDING),
        b'i' => Some(CAN_ID_POWER_ON_CAMERA),
        b'o' => Some(CAN_ID_POWER_OFF_CAMERA),
        b'a' => Some(CAN_ID_EMERGENCY_STOP_PARA),
        _ => None,
    }
}
#[embassy_executor::task]
async fn command_process_task() {
    loop {
        let command = RECEIVED_DATA_CHANNEL.receive().await;
        match command {
            // シーケンス制御
            b's' => {
                IS_LOGGING.store(true, Ordering::Relaxed);
                GNSS_CMD_CHANNEL.send(GnssCommand::TurnOn).await;
            }
            b'e' => IS_LOGGING.store(false, Ordering::Relaxed),

            // ロギング開始
            b'l' => IS_LOGGING.store(true, Ordering::Relaxed),
            // ロギング停止
            b'm' => IS_LOGGING.store(false, Ordering::Relaxed),

            // GNSS電源制御
            b'g' => GNSS_CMD_CHANNEL.send(GnssCommand::TurnOn).await,
            b'h' => GNSS_CMD_CHANNEL.send(GnssCommand::TurnOff).await,

            // CANバスへ流すコマンド
            _ => {
                if let Some(can_id) = get_target_can_id(command) {
                    CAN_TX_CHANNEL.send((can_id, command)).await;
                }
            }
        }
    }
}

#[embassy_executor::task]
async fn can_transmit_task(mut tx: twai::TwaiTx<'static, Async>) {
    loop {
        match select(
            CAN_TX_CHANNEL.receive(),
            Timer::after(Duration::from_secs(60)),
        )
        .await
        {
            // コマンド処理タスクからの送信依頼が来た場合
            Either::First((can_id, command)) => {
                let frame = create_can_frame_to_send(can_id, command);
                // 100msでタイムアウトさせる
                let result = embassy_time::with_timeout(
                    Duration::from_millis(100),
                    tx.transmit_async(&frame),
                )
                .await;

                // if result.is_err() {
                //     println!("CAN Timeout ID {}", can_id);
                // }
            }

            // 60秒経過した場合（
            Either::Second(_) => {
                let _ = embassy_time::with_timeout(
                    Duration::from_millis(100),
                    tx.transmit_async(&create_can_frame_to_send(CAN_ID_TEST_TO_LOG_PARA, 0)),
                )
                .await;

                let _ = embassy_time::with_timeout(
                    Duration::from_millis(100),
                    tx.transmit_async(&create_can_frame_to_send(CAN_ID_TEST_TO_CAMERA, 0)),
                )
                .await;

                let _ = embassy_time::with_timeout(
                    Duration::from_millis(100),
                    tx.transmit_async(&create_can_frame_to_send(CAN_ID_TEST_TO_POWER_CONTROL, 0)),
                )
                .await;
            }
        }
    }
}

#[embassy_executor::task]
async fn can_receive_task(mut rx: twai::TwaiRx<'static, Async>) {
    loop {
        let frame = rx.receive_async().await;
        match frame {
            Ok(payload) => {
                IS_CAN_ERROR.store(false, Ordering::Relaxed);
                match payload.id() {
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_LIFT_OFF => {
                        TRIGGER_SIGNAL.signal(true);
                        let mut status_payload = PAYLOAD_MUTEX.lock().await.status;
                        status_payload = (status_payload & 0b1011_1111) | 0b0100_0000;
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_ANGLE_SPEED => {
                        let mut angle_speed = [0u8; 6];
                        angle_speed.copy_from_slice(&payload.data()[0..6]);
                        let mut angle_speed_payload = PAYLOAD_MUTEX.lock().await.angle_speed;
                        for (i, chunk) in angle_speed.chunks_exact(2).enumerate() {
                            // chunk は [u8; 2] に変換（try_into）してから i16 にする
                            angle_speed_payload[i] = i16::from_le_bytes(chunk.try_into().unwrap());
                        }
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_ACCELARATION => {
                        let mut accelaration = [0u8; 6];
                        accelaration.copy_from_slice(&payload.data()[0..6]);
                        let mut accelaration_payload = PAYLOAD_MUTEX.lock().await.acceleration;
                        for (i, chunk) in accelaration.chunks_exact(2).enumerate() {
                            // chunk は [u8; 2] に変換（try_into）してから i16 にする
                            accelaration_payload[i] = i16::from_le_bytes(chunk.try_into().unwrap());
                        }
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_AIR_PRESSURE => {
                        let mut air_pressure = [0u8; 2];
                        air_pressure.copy_from_slice(&payload.data()[0..2]);
                        let mut air_pressure_payload = PAYLOAD_MUTEX.lock().await.air_pressure;
                        air_pressure_payload = i16::from_le_bytes(air_pressure.try_into().unwrap());
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_TOP => {
                        TRIGGER_SIGNAL.signal(true);
                        let mut status_payload = PAYLOAD_MUTEX.lock().await.status;
                        status_payload = (status_payload & 0b0111_1111) | 0b1000_0000;
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_CAMERA_STATUS => {
                        let status = payload.data()[0];
                        let mut status_payload = PAYLOAD_MUTEX.lock().await.status;

                        // (*status_payload & 0b1111_1000) -> 上位5bitだけ残して下位3bitを0にする
                        // (status & 0b0000_0111) -> 受け取ったデータの下位3bitだけ抽出する
                        status_payload = (status_payload & 0b1111_1000) | (status & 0b0000_0111);
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_TEST_FROM_LOG_PARA => {
                        let mut status_payload = PAYLOAD_MUTEX.lock().await.status;
                        status_payload = (status_payload & 0b1111_0111) | 0b0000_1000;
                        // 受信時刻を更新
                        *LAST_SEEN_LOG.lock().await = Some(Instant::now());
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_TEST_FROM_CAMERA => {
                        let mut status_payload = PAYLOAD_MUTEX.lock().await.status;
                        status_payload = (status_payload & 0b1110_1111) | 0b0001_0000;
                        // 受信時刻を更新
                        *LAST_SEEN_CAMERA.lock().await = Some(Instant::now());
                    }
                    Id::Standard(s_id) if s_id.as_raw() == CAN_ID_TEST_FROM_POWER_CONTROL => {
                        let status = payload.data()[0];
                        let mut status_payload = PAYLOAD_MUTEX.lock().await.status;
                        status_payload = (status_payload & 0b1101_1111) | 0b0010_0000;
                        // 受信時刻を更新
                        *LAST_SEEN_POWER.lock().await = Some(Instant::now());
                    }
                    _ => {} // ignore the others
                }
            }
            Err(e) => {
                // CAN受信エラー時はLEDを点滅させる
                IS_CAN_ERROR.store(true, Ordering::Relaxed);
                println!("CAN receive error: {:?}", e);
            }
        }
    }
}

#[embassy_executor::task]
async fn lora_task(mut uart: Uart<'static, Async>, mut aux_pin: Input<'static>) {
    let mut is_tx_only_mode = false;
    let mut rx_buf = [0u8; 64];

    loop {
        if !is_tx_only_mode {
            // --- 送受信モード (受信メイン) ---
            let trigger_fut = TRIGGER_SIGNAL.wait();
            let rx_fut = uart.read_async(&mut rx_buf);

            // 3秒経過したら送信を行うタイマー
            let tx_timer_fut = Timer::after(Duration::from_secs(3));

            // 3つのイベントを同時に待ち受け、最初に完了したものを処理する
            match select3(trigger_fut, rx_fut, tx_timer_fut).await {
                Either3::First(trigger) => {
                    if trigger {
                        is_tx_only_mode = true;
                    }
                }
                Either3::Second(Ok(_)) => {
                    // 受信処理
                    // rx_bufの0番目の値(u8)を取り出して、そのまま送信
                    RECEIVED_DATA_CHANNEL.send(rx_buf[0]).await;
                }
                Either3::Second(Err(_)) => {
                    // UART受信エラー時の処理
                }
                Either3::Third(_) => {
                    // 送信処理
                    let mut payload = Payload::new();
                    {
                        let payload_guard = PAYLOAD_MUTEX.lock().await;
                        payload = *payload_guard;
                    }
                    let payload_bytes = payload.to_bytes();
                    let _ = uart.write_async(&payload_bytes).await;
                    let _ = uart.flush();

                    // UARTに書き込んだ後、実際にLoRaで送信完了するまでAUXピンがLOW
                    aux_pin.wait_for_high().await;
                }
            }
        } else {
            // 送信専用モード
            let mut payload = Payload::new();
            {
                let payload_guard = PAYLOAD_MUTEX.lock().await;
                payload = *payload_guard;
            }
            let payload_bytes = payload.to_bytes();
            let _ = uart.write_async(&payload_bytes).await;
            let _ = uart.flush();

            // AUXピンがHIGHになるのを待機（送信完了待ち）
            aux_pin.wait_for_high().await;
        }
    }
}

#[embassy_executor::task]
async fn create_lora_payload() {
    loop {
        let mut local_payload = { *PAYLOAD_MUTEX.lock().await };
        let now = Instant::now();
        let is_timeout = |last_seen: Option<Instant>| -> bool {
            match last_seen {
                Some(time) => now.duration_since(time).as_secs() > 80,
                None => true, // まだ1度も受信していない場合も異常（初期状態）とみなす
            }
        };

        if is_timeout(*LAST_SEEN_LOG.lock().await) {
            local_payload.status &= 0b1111_0111; // ログ基板タイムアウト
        }
        if is_timeout(*LAST_SEEN_CAMERA.lock().await) {
            local_payload.status &= 0b1110_1111; // カメラ基板タイムアウト
        }
        if is_timeout(*LAST_SEEN_POWER.lock().await) {
            local_payload.status &= 0b1101_1111; // 電源基板タイムアウト
        }
        local_payload.update_checksum();

        {
            let mut global_payload = PAYLOAD_MUTEX.lock().await;
            *global_payload = local_payload;
        }
        Timer::after(Duration::from_millis(100)).await;
    }
}

#[embassy_executor::task]
pub async fn gnss_manager_task(mut uart: Uart<'static, Async>, mut gnss_en: Output<'static>) {
    let mut read_buf = [0u8; 90];
    let mut line_buf = [0u8; 90];
    let mut line_len = 0;

    loop {
        // UARTの受信とコマンドの受信を同時に待ち受ける
        match select(uart.read_async(&mut read_buf), GNSS_CMD_CHANNEL.receive()).await {
            //  GNSSからデータを受信した場合
            Either::First(Ok(bytes_read)) => {
                if bytes_read == 0 {
                    continue;
                }
                for &letter in &read_buf[..bytes_read] {
                    if letter == b'\r' {
                        continue;
                    }
                    if letter == b'\n' {
                        // 送信用に0で初期化された新しい配列を作る
                        let mut send_buf = [0u8; 90];
                        // line_buf から有効な長さ分だけ、send_bufの先頭にコピーする
                        send_buf[..line_len].copy_from_slice(&line_buf[..line_len]);
                        GNSS_CHANNEL.send(send_buf).await;
                        if let Err(_) = RAW_GNSS_CHANNEL.try_send(send_buf) {
                            // 満杯の場合は1つ取り出して空きを作り、再度入れる
                            let _ = RAW_GNSS_CHANNEL.try_receive();
                            let _ = RAW_GNSS_CHANNEL.try_send(send_buf);
                        }
                        // 次の行のためにリセット
                        line_len = 0;
                        continue;
                    }
                    if line_len < line_buf.len() {
                        line_buf[line_len] = letter;
                        line_len += 1;
                    } else {
                        line_len = 0;
                    }
                }
            }
            Either::First(Err(_)) => {} // UART受信エラー

            //  コマンド (ON/OFF) を受信した場合
            Either::Second(cmd) => {
                match cmd {
                    GnssCommand::TurnOn => {
                        gnss_en.set_high();

                        // 起動直後はデフォルトの9600bpsに戻す
                        let config_9600 = UartConfig::default().with_baudrate(9600);
                        uart.apply_config(&config_9600).unwrap();

                        Timer::after(Duration::from_millis(500)).await;

                        // GNSSモジュールに設定コマンド(115200への変更等)を送信
                        gnss_setting(&mut uart).await;

                        // GNSS側が115200bpsに切り替わるまで少し待機 (重要)
                        Timer::after(Duration::from_millis(50)).await;

                        // ESP32側のUARTも115200bpsに変更する
                        let config_115200 = UartConfig::default().with_baudrate(115_200);
                        uart.apply_config(&config_115200).unwrap();
                    }
                    GnssCommand::TurnOff => {
                        gnss_en.set_low();
                    }
                }
            }
        }
    }
}

#[embassy_executor::task]
async fn parse_gnss_task() {
    loop {
        // 90バイトの配列を受け取る
        let sentence = GNSS_CHANNEL.receive().await;
        match parse_rmc_movement(sentence.as_slice()) {
            Ok(rmc_data) => {
                if let (Some(speed), Some(degree)) = (rmc_data.speed_kmh, rmc_data.true_course) {
                    let velocity = [speed, degree];
                    RMC_CHANNEL.send(velocity).await;
                }
            }
            Err(_e) => {
                // パース失敗やrmc以外のセンテンスだった場合の処理
            }
        }
        match parse_gga(sentence.as_slice()) {
            Ok(gga_data) => {
                if let (Some(lat), Some(lon)) = (gga_data.latitude, gga_data.longitude) {
                    let location = [lat, lon];
                    let mut payload_gnss = PAYLOAD_MUTEX.lock().await;
                    [payload_gnss.gnss_lat, payload_gnss.gnss_long] = location;
                }
            }
            Err(_e) => {
                // パース失敗やGGA以外のセンテンスだった場合の処理
            }
        }
    }
}

#[embassy_executor::task]
async fn sd_write_task(
    mut volume_mgr: &'static mut VolumeManager<
        SdCard<ExclusiveDevice<Spi<'static, Async>, Output<'static>, Delay>, Delay>,
        SdTimeSource,
    >,
) {
    let mut raw_buffer = [0u8; BUF_SIZE]; // raw gnss data
    let mut tlm_buffer = [0u8; BUF_SIZE]; // payload + time stamp
    let mut raw_cursor = 0;
    let mut tlm_cursor = 0;
    let volume0 = volume_mgr.open_volume(VolumeIdx(0)).unwrap();
    let root_dir = volume0.open_root_dir().unwrap();
    let mut raw_gnss_file = root_dir
        .open_file_in_dir(
            "GNSS_RAW.TXT",
            embedded_sdmmc::Mode::ReadWriteCreateOrTruncate,
        )
        .unwrap();
    let mut tlm_file = root_dir
        .open_file_in_dir(
            "TELEMETRY.CSV",
            embedded_sdmmc::Mode::ReadWriteCreateOrTruncate,
        )
        .unwrap();
    // CSVのヘッダー
    let header = b"Time_ms,Status,Lat,Long,GyroX,GyroY,GyroZ,AccX,AccY,AccZ,Press,Speed,Course\n";
    let _ = tlm_file.write(header);
    loop {
        let flush_timer = Timer::after(Duration::from_secs(5));
        let has_data = raw_cursor > 0 || tlm_cursor > 0;
        HAS_UNFLUSHED_DATA.store(has_data, Ordering::Relaxed);
        let is_logging = IS_LOGGING.load(Ordering::Relaxed);
        match select3(
            RAW_GNSS_CHANNEL.receive(),
            RMC_CHANNEL.receive(),
            flush_timer,
        )
        .await
        {
            //  生のGNSSデータを受信した場合 (GNSS_RAW.TXT )
            Either3::First(raw_gnss_data) => {
                if is_logging {
                    // 後半の 0 埋め部分を無視して有効な長さを取得
                    let valid_len = raw_gnss_data
                        .iter()
                        .position(|&x| x == 0)
                        .unwrap_or(raw_gnss_data.len());
                    let valid_bytes = &raw_gnss_data[..valid_len];

                    // バッファが溢れそうならSDに書き出してカーソルをリセット
                    if raw_cursor + valid_bytes.len() > BUF_SIZE {
                        let _ = raw_gnss_file.write(&raw_buffer[..raw_cursor]);
                        let _ = raw_gnss_file.flush();
                        raw_cursor = 0;
                    }

                    // バッファに追記
                    raw_buffer[raw_cursor..raw_cursor + valid_bytes.len()]
                        .copy_from_slice(valid_bytes);
                    raw_cursor += valid_bytes.len();
                }
            }

            // RMCデータ(速度と方位)を受信した場合 (TELEMETRY.CSV へ書き込み)
            Either3::Second([speed, course]) => {
                if is_logging {
                    let payload = { *PAYLOAD_MUTEX.lock().await };
                    let now_ms = Instant::now().as_millis();

                    let status = payload.status;
                    let lat = payload.gnss_lat;
                    let long = payload.gnss_long;
                    let gyro_x = payload.angle_speed[0];
                    let gyro_y = payload.angle_speed[1];
                    let gyro_z = payload.angle_speed[2];
                    let acc_x = payload.acceleration[0];
                    let acc_y = payload.acceleration[1];
                    let acc_z = payload.acceleration[2];
                    let press = payload.air_pressure;

                    // CSV形式の文字列を組み立てる
                    let mut csv_line: String<256> = String::new();
                    let _ = write!(
                        &mut csv_line,
                        "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                        now_ms,
                        status,
                        lat,
                        long,
                        gyro_x,
                        gyro_y,
                        gyro_z,
                        acc_x,
                        acc_y,
                        acc_z,
                        press,
                        speed,
                        course
                    );

                    let line_bytes = csv_line.as_bytes();

                    // バッファが溢れそうならSDに書き出してカーソルをリセット
                    if tlm_cursor + line_bytes.len() > BUF_SIZE {
                        let _ = tlm_file.write(&tlm_buffer[..tlm_cursor]);
                        let _ = tlm_file.flush();
                        tlm_cursor = 0;
                    }

                    // バッファに追記
                    tlm_buffer[tlm_cursor..tlm_cursor + line_bytes.len()]
                        .copy_from_slice(line_bytes);
                    tlm_cursor += line_bytes.len();
                }
            }
            Either3::Third(_) => {
                // GNSS生データのバッファに何か溜まっていればSDに書き込んでフラッシュ
                if raw_cursor > 0 {
                    let _ = raw_gnss_file.write(&raw_buffer[..raw_cursor]);
                    let _ = raw_gnss_file.flush();
                    raw_cursor = 0;
                }
                // テレメトリデータのバッファに何か溜まっていればSDに書き込んでフラッシュ
                if tlm_cursor > 0 {
                    let _ = tlm_file.write(&tlm_buffer[..tlm_cursor]);
                    let _ = tlm_file.flush();
                    tlm_cursor = 0;
                }
            }
        }
    }
}

#[esp_rtos::main]
async fn main(spawner0: Spawner) -> ! {
    esp_println::logger::init_logger_from_env();
    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(config);
    // generator version: 1.2.0

    static APP_CORE_STACK: StaticCell<Stack<8192>> = StaticCell::new();
    let app_core_stack = APP_CORE_STACK.init(Stack::new());
    let sw_int = SoftwareInterruptControl::new(peripherals.SW_INTERRUPT);

    let timg0 = TimerGroup::new(peripherals.TIMG0);
    esp_rtos::start(timg0.timer0);
    // --- GPIO Definitions
    let can_tx = Output::new(peripherals.GPIO7, Level::Low, OutputConfig::default());
    let can_rx = Input::new(peripherals.GPIO16, InputConfig::default());
    let mut led1 = Output::new(peripherals.GPIO6, Level::Low, OutputConfig::default());
    let mut led2 = Output::new(peripherals.GPIO15, Level::Low, OutputConfig::default());
    let lora_tx = Output::new(peripherals.GPIO11, Level::Low, OutputConfig::default());
    let mut m0 = Output::new(peripherals.GPIO9, Level::Low, OutputConfig::default());
    let mut m1 = Output::new(peripherals.GPIO10, Level::Low, OutputConfig::default());
    let gnss_tx = Output::new(peripherals.GPIO21, Level::Low, OutputConfig::default());
    let mut gnss_en = Output::new(peripherals.GPIO13, Level::Low, OutputConfig::default());
    let aux_pin = Input::new(peripherals.GPIO8, InputConfig::default());
    let lora_rx = Input::new(peripherals.GPIO12, InputConfig::default());
    let gnss_rx = Input::new(peripherals.GPIO14, InputConfig::default());

    // --- SPI ---
    let spi_bus = Spi::new(
        peripherals.SPI2,
        spi::master::Config::default()
            .with_frequency(Rate::from_khz(400))
            .with_mode(spi::Mode::_0),
    )
    .unwrap()
    .with_sck(peripherals.GPIO41)
    .with_mosi(peripherals.GPIO42)
    .with_miso(peripherals.GPIO40)
    .into_async();
    // init sd
    let sd_cs = Output::new(peripherals.GPIO2, Level::High, OutputConfig::default());
    let spi_dev = ExclusiveDevice::new(spi_bus, sd_cs, Delay).unwrap();
    let rtc = Rtc::new(peripherals.LPWR);
    let sd_timer = SdTimeSource::new(rtc);
    let sdcard = SdCard::new(spi_dev, Delay);
    let sd_size = sdcard.num_bytes().unwrap();
    // To do this we need a Volume Manager. It will take ownership of the block device.
    static VOLUME_MGR: StaticCell<
        VolumeManager<
            SdCard<ExclusiveDevice<Spi<'static, Async>, Output<'static>, Delay>, Delay>,
            SdTimeSource,
        >,
    > = StaticCell::new();
    let volume_mgr = VOLUME_MGR.init(VolumeManager::new(sdcard, sd_timer));
    // Try and access Volume 0 (i.e. the first partition).
    // The volume object holds information about the filesystem on that volume.
    m0.set_low();
    m1.set_low();

    // init gnss_module
    // UART1設定
    let uart_config1 = UartConfig::default()
        .with_baudrate(9600)
        .with_data_bits(DataBits::_8)
        .with_parity(Parity::Even)
        .with_stop_bits(StopBits::_1);
    let mut uart1 = Uart::new(peripherals.UART1, uart_config1)
        .unwrap()
        .with_rx(gnss_rx)
        .with_tx(gnss_tx)
        .into_async();

    // spawn tasks on core0
    spawner0.spawn(gnss_manager_task(uart1, gnss_en)).ok();
    spawner0.spawn(sd_write_task(volume_mgr)).ok();
    spawner0.spawn(command_process_task()).ok();
    // spawn tasks on core1
    esp_rtos::start_second_core(
        peripherals.CPU_CTRL,
        #[cfg(target_arch = "xtensa")]
        sw_int.software_interrupt0,
        sw_int.software_interrupt1,
        app_core_stack,
        move || {
            static EXECUTOR: StaticCell<Executor> = StaticCell::new();
            let executor = EXECUTOR.init(Executor::new());
            executor.run(|spawner| {
                // CAN設定
                let mut can_config = twai::TwaiConfiguration::new(
                    peripherals.TWAI0,
                    can_rx,
                    can_tx,
                    BaudRate::B125K,
                    TwaiMode::Normal,
                )
                .into_async();
                // Partially filter the incoming messages to reduce overhead of receiving
                // undesired messages
                can_config.set_filter(
                        const {
                            SingleStandardFilter::new(
                                b"0xxxxxxxxxx",
                                b"x",
                                [b"xxxxxxxx", b"xxxxxxxx"],
                            )
                        },
                    );
                let can = can_config.start();
                let (rx, tx) = can.split();
                // init lora
                // UART2設定
                let uart_config2 = UartConfig::default()
                    .with_baudrate(9600)
                    .with_data_bits(DataBits::_8)
                    .with_parity(Parity::Even)
                    .with_stop_bits(StopBits::_1);
                let uart2 = Uart::new(peripherals.UART2, uart_config2)
                    .unwrap()
                    .with_rx(lora_rx)
                    .with_tx(lora_tx)
                    .into_async();
                spawner.spawn(can_receive_task(rx)).ok();
                spawner.spawn(can_transmit_task(tx)).ok();
                spawner.spawn(parse_gnss_task()).ok();
                spawner.spawn(lora_task(uart2, aux_pin)).ok();
            });
        },
    );
    loop {
        // led タスク
        let is_can_error = IS_CAN_ERROR.load(Ordering::Relaxed);

        if is_can_error {
            // エラーなら100msごとにON/OFFが反転（高速点滅）
            led1.toggle();
        } else {
            // 正常なら消灯したまま
            led1.set_low();
        }
        let is_logging = IS_LOGGING.load(Ordering::Relaxed);
        // lock().await が不要で、一瞬で値を読み取れます
        let has_unflushed = HAS_UNFLUSHED_DATA.load(Ordering::Relaxed);

        if is_logging || has_unflushed {
            led2.set_high(); // ロギング中 or 保存待ちデータがあれば点灯
        } else {
            led2.set_low(); // 完全に保存が終わっていれば消灯
        }

        // ループがCPUを独占しないように、100msごとに実行する
        Timer::after(Duration::from_millis(100)).await;
    }
}
