use crate::errors::{QuackError, Result};
use crate::vector::{DateValue, TimeUnit, TimestampUnit, Value, decimal_to_string};

#[derive(Clone, Debug, PartialEq)]
pub enum SqlParameter {
    Value(Value),
    List(Vec<SqlParameter>),
}

impl From<Value> for SqlParameter {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}

impl From<&str> for SqlParameter {
    fn from(value: &str) -> Self {
        Self::Value(Value::String(value.to_string()))
    }
}

impl From<String> for SqlParameter {
    fn from(value: String) -> Self {
        Self::Value(Value::String(value))
    }
}

impl From<i32> for SqlParameter {
    fn from(value: i32) -> Self {
        Self::Value(Value::Int(value as i64))
    }
}

impl From<i64> for SqlParameter {
    fn from(value: i64) -> Self {
        Self::Value(Value::Int(value))
    }
}

impl From<u64> for SqlParameter {
    fn from(value: u64) -> Self {
        Self::Value(Value::UInt(value))
    }
}

impl From<bool> for SqlParameter {
    fn from(value: bool) -> Self {
        Self::Value(Value::Bool(value))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlParameters {
    Positional(Vec<SqlParameter>),
    Named(indexmap::IndexMap<String, SqlParameter>),
}

pub fn format_sql(sql: &str, params: Option<&SqlParameters>) -> Result<String> {
    match params {
        None => Ok(sql.to_string()),
        Some(SqlParameters::Positional(params)) => format_positional_sql(sql, params),
        Some(SqlParameters::Named(params)) => format_named_sql(sql, params),
    }
}

pub(crate) struct QuerySql {
    raw: String,
    preview: String,
}

impl QuerySql {
    const HEAD_LEN: usize = 10;
    const TAIL_LEN: usize = 10;

    pub(crate) fn new(raw: String) -> Self {
        let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        let chars: Vec<char> = collapsed.chars().collect();

        let preview = if chars.len() <= Self::HEAD_LEN + Self::TAIL_LEN {
            collapsed
        } else {
            format!(
                "{}...{}",
                chars[..Self::HEAD_LEN].iter().collect::<String>(),
                chars[chars.len() - Self::TAIL_LEN..]
                    .iter()
                    .collect::<String>()
            )
        };

        Self { raw, preview }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.raw
    }
}

impl std::fmt::Display for QuerySql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.preview)
    }
}

pub fn sql_literal(value: &SqlParameter) -> Result<String> {
    match value {
        SqlParameter::List(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(sql_literal)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        )),
        SqlParameter::Value(value) => value_literal(value),
    }
}

fn value_literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::HugeInt(value) => value.to_string(),
        Value::UHugeInt(value) => value.to_string(),
        Value::Float(value) if value.is_finite() => value.to_string(),
        Value::Double(value) if value.is_finite() => value.to_string(),
        Value::Float(value) => {
            return Err(QuackError::protocol(format!(
                "cannot encode non-finite SQL number {value}"
            )));
        }
        Value::Double(value) => {
            return Err(QuackError::protocol(format!(
                "cannot encode non-finite SQL number {value}"
            )));
        }
        Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Bytes(value) => format!("from_hex('{}')", bytes_to_hex(value)),
        Value::Decimal(value) => decimal_to_string(value),
        Value::Date(value) => format!("DATE '{}'", date_from_days(*value)),
        Value::Time(value) => format!("TIME '{}'", format_time(value.value, value.unit)),
        Value::TimeTz(_) => {
            return Err(QuackError::protocol(
                "TIME WITH TIME ZONE parameters are not supported as SQL literals",
            ));
        }
        Value::Timestamp(value) => {
            let keyword = if value.timezone_utc {
                "TIMESTAMPTZ"
            } else {
                "TIMESTAMP"
            };
            format!(
                "{keyword} '{}'",
                format_timestamp(value.value, value.unit, value.timezone_utc)
            )
        }
        Value::Interval(value) => format!(
            "INTERVAL '{} months {} days {} microseconds'",
            value.months, value.days, value.micros
        ),
        Value::List(_) | Value::Struct(_) => {
            return Err(QuackError::protocol(
                "object SQL parameters are not supported; pass scalar values or SqlParameter::List",
            ));
        }
    })
}

fn format_positional_sql(sql: &str, params: &[SqlParameter]) -> Result<String> {
    let mut index = 0usize;
    let formatted = scan_sql(sql, |token| {
        if token != "?" {
            return Ok(token.to_string());
        }
        let value = params.get(index).ok_or_else(|| {
            QuackError::protocol("SQL has more positional placeholders than parameters")
        })?;
        index += 1;
        sql_literal(value)
    })?;
    if index != params.len() {
        return Err(QuackError::protocol(format!(
            "SQL has {index} positional placeholders but {} parameters were provided",
            params.len()
        )));
    }
    Ok(formatted)
}

fn format_named_sql(
    sql: &str,
    params: &indexmap::IndexMap<String, SqlParameter>,
) -> Result<String> {
    scan_sql(sql, |token| {
        if !token.starts_with(':') {
            return Ok(token.to_string());
        }
        let name = &token[1..];
        let value = params
            .get(name)
            .ok_or_else(|| QuackError::protocol(format!("missing SQL parameter :{name}")))?;
        sql_literal(value)
    })
}

fn scan_sql(sql: &str, mut replace_token: impl FnMut(&str) -> Result<String>) -> Result<String> {
    let bytes = sql.as_bytes();
    let mut output = String::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        let next = bytes.get(index + 1).copied().map(char::from);
        if ch == '\'' {
            let end = read_single_quoted(sql, index);
            output.push_str(&sql[index..end]);
            index = end;
            continue;
        }
        if ch == '"' {
            let end = read_double_quoted(sql, index);
            output.push_str(&sql[index..end]);
            index = end;
            continue;
        }
        if ch == '-' && next == Some('-') {
            let end = sql[index + 2..]
                .find('\n')
                .map(|relative| index + 2 + relative)
                .unwrap_or(sql.len());
            output.push_str(&sql[index..end]);
            index = end;
            continue;
        }
        if ch == '/' && next == Some('*') {
            let end = sql[index + 2..]
                .find("*/")
                .map(|relative| index + 2 + relative + 2)
                .unwrap_or(sql.len());
            output.push_str(&sql[index..end]);
            index = end;
            continue;
        }
        if ch == '?' {
            output.push_str(&replace_token("?")?);
            index += 1;
            continue;
        }
        if ch == ':'
            && index.checked_sub(1).and_then(|i| bytes.get(i)).copied() != Some(b':')
            && next != Some(':')
            && next.is_some_and(is_identifier_start)
        {
            let start = index + 1;
            let mut end = start + 1;
            while end < bytes.len() && is_identifier_part(bytes[end] as char) {
                end += 1;
            }
            output.push_str(&replace_token(&sql[index..end])?);
            index = end;
            continue;
        }
        output.push(ch);
        index += 1;
    }
    Ok(output)
}

fn read_single_quoted(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'\'' && bytes.get(index + 1) == Some(&b'\'') {
            index += 2;
            continue;
        }
        if bytes[index] == b'\'' {
            return index + 1;
        }
        index += 1;
    }
    index
}

fn read_double_quoted(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'"' && bytes.get(index + 1) == Some(&b'"') {
            index += 2;
            continue;
        }
        if bytes[index] == b'"' {
            return index + 1;
        }
        index += 1;
    }
    index
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn date_from_days(value: DateValue) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    (epoch + chrono::Duration::days(value.days as i64))
        .format("%Y-%m-%d")
        .to_string()
}

fn format_timestamp(value: i64, unit: TimestampUnit, timezone_utc: bool) -> String {
    let micros = match unit {
        TimestampUnit::Seconds => value * 1_000_000,
        TimestampUnit::Millis => value * 1_000,
        TimestampUnit::Micros => value,
        TimestampUnit::Nanos => value / 1_000,
    };
    let seconds = micros.div_euclid(1_000_000);
    let micro_fraction = micros.rem_euclid(1_000_000);
    let date = chrono::DateTime::from_timestamp(seconds, (micro_fraction as u32) * 1_000)
        .expect("timestamp in range");
    let suffix = if timezone_utc { "+00" } else { "" };
    format!(
        "{}.{:03}{suffix}",
        date.format("%Y-%m-%d %H:%M:%S"),
        micro_fraction / 1_000
    )
}

fn format_time(value: i64, unit: TimeUnit) -> String {
    let nanos = match unit {
        TimeUnit::Micros => value as i128 * 1_000,
        TimeUnit::Nanos => value as i128,
    };
    let total_seconds = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    let fraction_text = if fraction == 0 {
        String::new()
    } else {
        format!(".{:09}", fraction)
            .trim_end_matches('0')
            .to_string()
    };
    format!("{hours:02}:{minutes:02}:{seconds:02}{fraction_text}")
}
