#![no_std]
use core::str::FromStr;

// --- エラー定義 ---
#[derive(Debug, PartialEq)]
pub enum GgaParseError {
    InvalidStart,
    MissingField,
    InvalidChecksum,
    ParseError,
    InvalidTalker,
    InvalidHemisphere,
    InvalidUnit,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum FixQuality {
    Invalid = 0,
    Gps = 1,
    Dgps = 2,
    Unknown(u8),
}

impl Default for FixQuality {
    fn default() -> Self {
        FixQuality::Invalid
    }
}

impl From<u8> for FixQuality {
    fn from(v: u8) -> Self {
        match v {
            0 => FixQuality::Invalid,
            1 => FixQuality::Gps,
            2 => FixQuality::Dgps,
            x => FixQuality::Unknown(x),
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct UtcTime {
    pub hour: u8,
    pub minute: u8,
    pub second: f32,
}

#[derive(Debug, Default, PartialEq)]
pub struct GgaData {
    pub utc_time: Option<UtcTime>,
    pub latitude: Option<i32>,
    pub longitude: Option<i32>,
    pub fix_quality: FixQuality,
    pub satellites: u8,
    pub altitude: Option<f32>,  // 海抜高度
    pub geoid_sep: Option<f32>, // ジオイド高
}
impl GgaData {
    /// ジオイド高を用いて「WGS84楕円体高」を計算して返す
    pub fn ellipsoid_height(&self) -> Option<f32> {
        if let (Some(alt), Some(geoid)) = (self.altitude, self.geoid_sep) {
            Some(alt + geoid) // 海抜高度 + ジオイド高 = 楕円体高
        } else {
            None
        }
    }
}

/// バイト配列から数値(f32, f64, u8等)をパースするヘルパー
fn parse_num<T: FromStr>(bytes: &[u8]) -> Result<T, GgaParseError> {
    // 必要な部分だけをstrに変換してパース（ASCII前提なので安全かつ高速）
    let s = core::str::from_utf8(bytes).map_err(|_| GgaParseError::ParseError)?;
    s.parse::<T>().map_err(|_| GgaParseError::ParseError)
}

/// hhmmss.ss形式のバイト配列 → UtcTime
fn parse_utc_time(bytes: &[u8]) -> Result<UtcTime, GgaParseError> {
    if bytes.len() < 6 {
        return Err(GgaParseError::ParseError);
    }
    let hour = parse_num::<u8>(&bytes[0..2])?;
    let minute = parse_num::<u8>(&bytes[2..4])?;
    let second = parse_num::<f32>(&bytes[4..])?;

    Ok(UtcTime {
        hour,
        minute,
        second,
    })
}

/// NMEA(ddmm.mmmm) → decimal degree
/// hemisphereは1バイト文字 (b'N', b'S', b'E', b'W')
fn nmea_to_decimal(nmea_val: f64, hemisphere: u8) -> Result<f64, GgaParseError> {
    // no_std環境のためキャストで整数部(度)を取得
    let degrees = (nmea_val / 100.0) as i32 as f64;
    let minutes = nmea_val - degrees * 100.0;
    let decimal = degrees + minutes / 60.0;

    match hemisphere {
        b'N' | b'E' => Ok(decimal),
        b'S' | b'W' => Ok(-decimal),
        _ => Err(GgaParseError::InvalidHemisphere),
    }
}
fn f64_to_gps_i32(degrees: f64) -> i32 {
    // 10,000,000を掛けて四捨五入し、i32にキャスト
    let scaled = degrees * 10_000_000.0;
    if scaled >= 0.0 {
        (scaled + 0.5) as i32
    } else {
        (scaled - 0.5) as i32
    }
}
/// チェックサム検証
fn verify_checksum(sentence: &[u8]) -> Result<usize, GgaParseError> {
    if sentence.is_empty() || sentence[0] != b'$' {
        return Err(GgaParseError::InvalidStart);
    }

    // '*' の位置を探す
    let star_idx = sentence
        .iter()
        .position(|&c| c == b'*')
        .ok_or(GgaParseError::MissingField)?;

    // 配列外アクセス(Panic)を防ぐための安全確認
    if star_idx + 3 > sentence.len() {
        return Err(GgaParseError::ParseError);
    }

    // '*'の直後の2bytesを切り出してパースする
    let hex_bytes = &sentence[star_idx + 1..star_idx + 3];
    let hex_str = core::str::from_utf8(hex_bytes).map_err(|_| GgaParseError::ParseError)?;
    let expected = u8::from_str_radix(hex_str, 16).map_err(|_| GgaParseError::ParseError)?;

    // XOR計算
    let mut calc = 0u8;
    for &b in &sentence[1..star_idx] {
        calc ^= b;
    }

    if calc != expected {
        return Err(GgaParseError::InvalidChecksum);
    }

    Ok(star_idx)
}

/// GxGGAセンテンスをパース
pub fn parse_gga(sentence: &[u8]) -> Result<GgaData, GgaParseError> {
    let star_idx = verify_checksum(sentence)?;

    let data_bytes = &sentence[1..star_idx];

    // b',' (カンマのバイト値) で分割
    let mut fields = data_bytes.split(|&b| b == b',');

    let talker = fields.next().ok_or(GgaParseError::MissingField)?;

    // バイト配列として "GGA" を比較
    if talker.len() < 5 || &talker[talker.len() - 3..] != b"GGA" {
        return Err(GgaParseError::InvalidTalker);
    }

    let time_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let lat_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let ns_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let lon_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let ew_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let fix_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let sat_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let _hdop = fields.next().ok_or(GgaParseError::MissingField)?;
    let alt_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    let alt_unit = fields.next().ok_or(GgaParseError::MissingField)?;
    let geoid_bytes = fields.next().ok_or(GgaParseError::MissingField)?;
    // 測位前は空文字 b"" ではなく、Mかどうかのチェック (b"M"と比較)
    if !alt_unit.is_empty() && alt_unit != b"M" {
        return Err(GgaParseError::InvalidUnit);
    }

    let mut data = GgaData::default();

    if !time_bytes.is_empty() {
        data.utc_time = Some(parse_utc_time(time_bytes)?);
    }

    if !lat_bytes.is_empty() && !ns_bytes.is_empty() {
        let raw = parse_num::<f64>(lat_bytes)?;
        let decimal = nmea_to_decimal(raw, ns_bytes[0])?;
        data.latitude = Some(f64_to_gps_i32(decimal));
    }

    if !lon_bytes.is_empty() && !ew_bytes.is_empty() {
        let raw = parse_num::<f64>(lon_bytes)?;
        let decimal = nmea_to_decimal(raw, ew_bytes[0])?;
        data.longitude = Some(f64_to_gps_i32(decimal));
    }

    if !fix_bytes.is_empty() {
        let v = parse_num::<u8>(fix_bytes)?;
        data.fix_quality = FixQuality::from(v);
    }

    if !sat_bytes.is_empty() {
        data.satellites = parse_num::<u8>(sat_bytes)?;
    }

    if !alt_bytes.is_empty() {
        data.altitude = Some(parse_num::<f32>(alt_bytes)?);
    }
    if !geoid_bytes.is_empty() {
        data.geoid_sep = Some(parse_num::<f32>(geoid_bytes)?); // ジオイド高のパース
    }

    Ok(data)
}

/// RMCから抽出する移動データ
#[derive(Debug, Default, PartialEq)]
pub struct RmcData {
    pub speed_kmh: Option<f32>,   // 対地速度 (km/h)
    pub true_course: Option<f32>, // 真方位 (度: 0.0=北, 90.0=東, 180.0=南, 270.0=西)
}

/// RMCセンテンスから対地速度と真方位だけを抽出する
pub fn parse_rmc_movement(sentence: &[u8]) -> Result<RmcData, GgaParseError> {
    let star_idx = verify_checksum(sentence)?;

    let data_bytes = &sentence[1..star_idx];
    let mut fields = data_bytes.split(|&b| b == b',');

    //  Talker ID
    let talker = fields.next().ok_or(GgaParseError::MissingField)?;
    if talker.len() < 5 || &talker[talker.len() - 3..] != b"RMC" {
        return Err(GgaParseError::InvalidTalker);
    }

    //  UTC時刻
    let _time = fields.next().ok_or(GgaParseError::MissingField)?;

    //  ステータス ('A' = 有効, 'V' = 無効)
    let status = fields.next().ok_or(GgaParseError::MissingField)?;
    if status != b"A" {
        // 測位無効の時はエラーとして弾く
        return Err(GgaParseError::ParseError);
    }

    // 緯度・経度などの中間4フィールドをスキップして、速度(7)を取得
    let speed_bytes = fields.nth(4).ok_or(GgaParseError::MissingField)?;

    // 速度のすぐ次が真方位(8)
    let course_bytes = fields.next().ok_or(GgaParseError::MissingField)?;

    let mut data = RmcData::default();

    // 速度のパースと km/h 変換
    if !speed_bytes.is_empty() {
        let speed_knots = parse_num::<f32>(speed_bytes)?;
        data.speed_kmh = Some(speed_knots * 1.852);
    }

    // 真方位のパース
    if !course_bytes.is_empty() {
        data.true_course = Some(parse_num::<f32>(course_bytes)?);
    }

    Ok(data)
}

// GNSS_moduleの設定バイナリデータ
pub const GGL_DELETE: &[u8] = &[
    0xB5, 0x62, 0x06, 0x01, 0x08, 0x00, 0xF0, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,
];
pub const GSA_DELETE: &[u8] = &[
    0xB5, 0x62, 0x06, 0x01, 0x08, 0x00, 0xF0, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x31,
];
pub const GSV_DELETE: &[u8] = &[
    0xB5, 0x62, 0x06, 0x01, 0x08, 0x00, 0xF0, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x38,
];
pub const VTG_DELETE: &[u8] = &[
    0xB5, 0x62, 0x06, 0x01, 0x08, 0x00, 0xF0, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x46,
];
pub const MEAS_RATE: &[u8] = &[
    0xB5, 0x62, 0x06, 0x08, 0x06, 0x00, 0x64, 0x00, 0x01, 0x00, 0x01, 0x00, 0x7A, 0x12,
]; // 1Hz -> 10Hzに変更
pub const SLAS_EN: &[u8] = &[
    0xB5, 0x62, 0x06, 0x8D, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x98, 0x27,
];
pub const DYNAMIC_MODEL_AIRBORNE_4G: &[u8] = &[
    0xB5, 0x62, 0x06, 0x24, 0x24, 0x00, 0xFF, 0xFF, 0x08, 0x03, 0x00, 0x00, 0x00, 0x00, 0x10, 0x27,
    0x00, 0x00, 0x0A, 0x00, 0xFA, 0x00, 0xFA, 0x00, 0x64, 0x00, 0x5E, 0x01, 0x00, 0x3C, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x8B, 0xC4,
]; // Airborne < 4g
pub const UART_BAUD: &[u8] = &[
    0xB5, 0x62, 0x06, 0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0xD0, 0x08, 0x00, 0x00, 0x00, 0xC2,
    0x01, 0x00, 0x03, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0xBC, 0x5E,
]; // baud rate = 115200

async fn send_cmd(tx: &mut esp_hal::uart::Uart<'static, esp_hal::Async>, mut data: &[u8]) {
    while !data.is_empty() {
        let written = tx.write_async(data).await.unwrap();
        // 書き込めた分だけスライスの先頭を削って、残りを次のループで送る
        data = &data[written..];
    }
}
pub async fn gnss_setting(tx: &mut esp_hal::uart::Uart<'static, esp_hal::Async>) {
    send_cmd(tx, GGL_DELETE).await;
    send_cmd(tx, GSA_DELETE).await;
    send_cmd(tx, GSV_DELETE).await;
    send_cmd(tx, VTG_DELETE).await;
    send_cmd(tx, MEAS_RATE).await;
    send_cmd(tx, SLAS_EN).await;
    send_cmd(tx, DYNAMIC_MODEL_AIRBORNE_4G).await;
    send_cmd(tx, UART_BAUD).await;
    tx.flush_async().await.unwrap();
}
