//! The `LogicalType` to Arrow `DataType` mapping.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, IntervalUnit, TimeUnit as ArrowTimeUnit};

use crate::errors::{QuackError, Result};
use crate::logical_types::{
    LogicalType, LogicalTypeId, get_array_size, get_child_type, get_decimal_width_and_scale,
    get_struct_children,
};

use super::{HUGE_INT_PRECISION, UTC_TIMEZONE, unsupported};

/// Maps a Quack logical type onto the Arrow type the bridge produces for it.
///
/// Three mappings are provisional and may change in a future release: `MAP` is
/// encoded as `List<Struct<key, value>>` rather than Arrow's native `Map`,
/// `ENUM` as plain `Utf8` rather than a `Dictionary`, and `VARIANT` as the
/// struct DuckDB shreds it into rather than Arrow's canonical `arrow.variant`
/// extension, whose binary encoding is not the one DuckDB sends.
pub(super) fn arrow_type(logical_type: &LogicalType) -> Result<DataType> {
    Ok(match logical_type.id {
        LogicalTypeId::SqlNull => DataType::Null,
        LogicalTypeId::Boolean => DataType::Boolean,
        LogicalTypeId::TinyInt => DataType::Int8,
        LogicalTypeId::SmallInt => DataType::Int16,
        LogicalTypeId::Integer => DataType::Int32,
        LogicalTypeId::BigInt => DataType::Int64,
        LogicalTypeId::UTinyInt => DataType::UInt8,
        LogicalTypeId::USmallInt => DataType::UInt16,
        LogicalTypeId::UInteger => DataType::UInt32,
        LogicalTypeId::UBigInt => DataType::UInt64,
        LogicalTypeId::HugeInt | LogicalTypeId::UHugeInt => {
            DataType::Decimal256(HUGE_INT_PRECISION, 0)
        }
        LogicalTypeId::Float => DataType::Float32,
        LogicalTypeId::Double => DataType::Float64,
        LogicalTypeId::Decimal => {
            let (precision, scale) = decimal_precision_and_scale(logical_type)?;
            DataType::Decimal128(precision, scale)
        }
        LogicalTypeId::Varchar
        | LogicalTypeId::Char
        | LogicalTypeId::Enum
        | LogicalTypeId::Uuid => DataType::Utf8,
        LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit => DataType::Binary,
        LogicalTypeId::Date => DataType::Date32,
        LogicalTypeId::Time => DataType::Time64(ArrowTimeUnit::Microsecond),
        LogicalTypeId::TimeNs => DataType::Time64(ArrowTimeUnit::Nanosecond),
        LogicalTypeId::TimestampSec => DataType::Timestamp(ArrowTimeUnit::Second, None),
        LogicalTypeId::TimestampMs => DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
        LogicalTypeId::Timestamp => DataType::Timestamp(ArrowTimeUnit::Microsecond, None),
        LogicalTypeId::TimestampNs => DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
        LogicalTypeId::TimestampTz => {
            DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(UTC_TIMEZONE.into()))
        }
        LogicalTypeId::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        LogicalTypeId::List | LogicalTypeId::Map => DataType::List(list_item_field(logical_type)?),
        LogicalTypeId::Array => {
            DataType::FixedSizeList(list_item_field(logical_type)?, array_size(logical_type)?)
        }
        LogicalTypeId::Struct | LogicalTypeId::Variant => {
            DataType::Struct(struct_fields(logical_type)?)
        }
        other => return Err(unsupported(other)),
    })
}

pub(super) fn field(name: &str, logical_type: &LogicalType) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, arrow_type(logical_type)?, true)))
}

fn list_item_field(logical_type: &LogicalType) -> Result<FieldRef> {
    Ok(Arc::new(Field::new_list_field(
        arrow_type(get_child_type(logical_type)?)?,
        true,
    )))
}

fn struct_fields(logical_type: &LogicalType) -> Result<Fields> {
    Ok(get_struct_children(logical_type)?
        .iter()
        .map(|child| field(&child.name, &child.logical_type))
        .collect::<Result<Vec<_>>>()?
        .into())
}

fn array_size(logical_type: &LogicalType) -> Result<i32> {
    let size = get_array_size(logical_type)?;
    i32::try_from(size)
        .map_err(|_| QuackError::protocol(format!("ARRAY size {size} exceeds the Arrow limit")))
}

pub(super) fn decimal_precision_and_scale(logical_type: &LogicalType) -> Result<(u8, i8)> {
    let (width, scale) = get_decimal_width_and_scale(logical_type)?;
    match (u8::try_from(width), i8::try_from(scale)) {
        (Ok(width), Ok(scale)) => Ok((width, scale)),
        _ => Err(QuackError::protocol(format!(
            "DECIMAL({width}, {scale}) exceeds the Arrow decimal limits"
        ))),
    }
}
