//! Extractors from a decoded `Value` to the native Arrow representation.

use arrow_array::temporal_conversions::{MICROSECONDS_IN_DAY, NANOSECONDS_IN_DAY};
use arrow_buffer::{IntervalMonthDayNano, i256};

use crate::errors::{QuackError, Result};
use crate::logical_types::LogicalTypeId;
use crate::vector::{DecimalValue, TimeUnit, TimestampUnit, Value};

use super::{mismatch, out_of_range};

const NANOS_PER_MICRO: i64 = 1_000;

pub(super) fn as_bool(id: LogicalTypeId, value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_int(id: LogicalTypeId, value: &Value) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_uint(id: LogicalTypeId, value: &Value) -> Result<u64> {
    match value {
        Value::UInt(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_huge_int(id: LogicalTypeId, value: &Value) -> Result<i256> {
    match value {
        Value::HugeInt(value) => Ok(i256::from_i128(*value)),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_unsigned_huge_int(id: LogicalTypeId, value: &Value) -> Result<i256> {
    match value {
        Value::UHugeInt(value) => Ok(i256::from_parts(*value, 0)),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_float(id: LogicalTypeId, value: &Value) -> Result<f32> {
    match value {
        Value::Float(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_double(id: LogicalTypeId, value: &Value) -> Result<f64> {
    match value {
        Value::Double(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_decimal(id: LogicalTypeId, value: &Value) -> Result<&DecimalValue> {
    match value {
        Value::Decimal(value) => Ok(value),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_str(id: LogicalTypeId, value: &Value) -> Result<&str> {
    match value {
        Value::String(value) => Ok(value.as_str()),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_bytes(id: LogicalTypeId, value: &Value) -> Result<&[u8]> {
    match value {
        Value::Bytes(value) => Ok(value.as_slice()),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_date(id: LogicalTypeId, value: &Value) -> Result<i32> {
    match value {
        Value::Date(value) => Ok(value.days),
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn narrow_int<T: TryFrom<i64>>(id: LogicalTypeId, value: &Value) -> Result<T> {
    let value = as_int(id, value)?;
    T::try_from(value).map_err(|_| out_of_range(id, value))
}

pub(super) fn narrow_uint<T: TryFrom<u64>>(id: LogicalTypeId, value: &Value) -> Result<T> {
    let value = as_uint(id, value)?;
    T::try_from(value).map_err(|_| out_of_range(id, value))
}

pub(super) fn as_time(id: LogicalTypeId, value: &Value, unit: TimeUnit) -> Result<i64> {
    match value {
        Value::Time(value) if value.unit == unit => {
            // DuckDB accepts TIME '24:00:00', which Arrow's "elapsed time since
            // midnight" bound excludes.
            let limit = match unit {
                TimeUnit::Micros => MICROSECONDS_IN_DAY,
                TimeUnit::Nanos => NANOSECONDS_IN_DAY,
            };
            if !(0..limit).contains(&value.value) {
                return Err(out_of_range(id, value.value));
            }
            Ok(value.value)
        }
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_timestamp(
    id: LogicalTypeId,
    value: &Value,
    unit: TimestampUnit,
    timezone_utc: bool,
) -> Result<i64> {
    match value {
        Value::Timestamp(value) if value.unit == unit && value.timezone_utc == timezone_utc => {
            Ok(value.value)
        }
        other => Err(mismatch(id, other)),
    }
}

pub(super) fn as_interval(id: LogicalTypeId, value: &Value) -> Result<IntervalMonthDayNano> {
    match value {
        Value::Interval(value) => {
            let nanoseconds = value.micros.checked_mul(NANOS_PER_MICRO).ok_or_else(|| {
                QuackError::protocol(format!(
                    "INTERVAL of {} microseconds overflows Arrow nanosecond resolution",
                    value.micros
                ))
            })?;
            Ok(IntervalMonthDayNano::new(
                value.months,
                value.days,
                nanoseconds,
            ))
        }
        other => Err(mismatch(id, other)),
    }
}
