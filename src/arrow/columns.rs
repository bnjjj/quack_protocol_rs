//! Column builders: one Arrow array per `DataChunk` column.

use std::iter::zip;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalMonthDayNanoType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BooleanArray, FixedSizeListArray, ListArray, NullArray,
    PrimitiveArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, FieldRef, Fields};

use crate::errors::{QuackError, Result};
use crate::logical_types::{LogicalType, LogicalTypeId, get_child_type, get_struct_children};
use crate::vector::{TimeUnit, TimestampUnit, Value};

use super::schema::decimal_precision_and_scale;
use super::values::{
    as_bool, as_bytes, as_date, as_decimal, as_double, as_float, as_huge_int, as_int, as_interval,
    as_str, as_time, as_timestamp, as_uint, as_unsigned_huge_int, narrow_int, narrow_uint,
};
use super::{HUGE_INT_PRECISION, UTC_TIMEZONE, arrow_error, mismatch, schema_shape, unsupported};

const NULL_VALUE: Value = Value::Null;

/// Builds one column. `data_type` is the schema's type for the column, so
/// nested builders reuse the schema's own field objects instead of rebuilding
/// an identical field tree for every chunk.
pub(super) fn build_array(
    logical_type: &LogicalType,
    data_type: &DataType,
    values: &[&Value],
) -> Result<ArrayRef> {
    let id = logical_type.id;
    Ok(match id {
        LogicalTypeId::SqlNull => {
            // A SQLNULL column carries nothing but nulls; a decoded value in
            // one would otherwise be dropped on the floor by `NullArray`.
            if let Some(value) = values.iter().find(|value| !matches!(value, Value::Null)) {
                return Err(mismatch(id, value));
            }
            Arc::new(NullArray::new(values.len()))
        }
        LogicalTypeId::Boolean => Arc::new(boolean(values, |value| as_bool(id, value))?),
        LogicalTypeId::TinyInt => Arc::new(primitive::<Int8Type>(values, |value| {
            narrow_int(id, value)
        })?),
        LogicalTypeId::SmallInt => Arc::new(primitive::<Int16Type>(values, |value| {
            narrow_int(id, value)
        })?),
        LogicalTypeId::Integer => Arc::new(primitive::<Int32Type>(values, |value| {
            narrow_int(id, value)
        })?),
        LogicalTypeId::BigInt => {
            Arc::new(primitive::<Int64Type>(values, |value| as_int(id, value))?)
        }
        LogicalTypeId::UTinyInt => Arc::new(primitive::<UInt8Type>(values, |value| {
            narrow_uint(id, value)
        })?),
        LogicalTypeId::USmallInt => Arc::new(primitive::<UInt16Type>(values, |value| {
            narrow_uint(id, value)
        })?),
        LogicalTypeId::UInteger => Arc::new(primitive::<UInt32Type>(values, |value| {
            narrow_uint(id, value)
        })?),
        LogicalTypeId::UBigInt => {
            Arc::new(primitive::<UInt64Type>(values, |value| as_uint(id, value))?)
        }
        LogicalTypeId::HugeInt => Arc::new(
            primitive::<Decimal256Type>(values, |value| as_huge_int(id, value))?
                .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                .map_err(arrow_error)?,
        ),
        LogicalTypeId::UHugeInt => Arc::new(
            primitive::<Decimal256Type>(values, |value| as_unsigned_huge_int(id, value))?
                .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                .map_err(arrow_error)?,
        ),
        LogicalTypeId::Float => Arc::new(primitive::<Float32Type>(values, |value| {
            as_float(id, value)
        })?),
        LogicalTypeId::Double => Arc::new(primitive::<Float64Type>(values, |value| {
            as_double(id, value)
        })?),
        LogicalTypeId::Decimal => {
            let (precision, scale) = decimal_precision_and_scale(logical_type)?;
            Arc::new(
                primitive::<Decimal128Type>(values, |value| {
                    as_decimal(id, value).map(|decimal| decimal.value)
                })?
                .with_precision_and_scale(precision, scale)
                .map_err(arrow_error)?,
            )
        }
        LogicalTypeId::Varchar
        | LogicalTypeId::Char
        | LogicalTypeId::Enum
        | LogicalTypeId::Uuid => {
            let strings = collect(values, |value| as_str(id, value))?;
            let mut builder =
                StringBuilder::with_capacity(strings.len(), byte_capacity(id, &strings)?);
            for value in &strings {
                builder.append_option(*value);
            }
            Arc::new(builder.finish())
        }
        LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit => {
            let blobs = collect(values, |value| as_bytes(id, value))?;
            let mut builder = BinaryBuilder::with_capacity(blobs.len(), byte_capacity(id, &blobs)?);
            for value in &blobs {
                builder.append_option(*value);
            }
            Arc::new(builder.finish())
        }
        LogicalTypeId::Date => {
            Arc::new(primitive::<Date32Type>(values, |value| as_date(id, value))?)
        }
        LogicalTypeId::Time => Arc::new(primitive::<Time64MicrosecondType>(values, |value| {
            as_time(id, value, TimeUnit::Micros)
        })?),
        LogicalTypeId::TimeNs => Arc::new(primitive::<Time64NanosecondType>(values, |value| {
            as_time(id, value, TimeUnit::Nanos)
        })?),
        LogicalTypeId::TimestampSec => {
            Arc::new(primitive::<TimestampSecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Seconds, false)
            })?)
        }
        LogicalTypeId::TimestampMs => {
            Arc::new(primitive::<TimestampMillisecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Millis, false)
            })?)
        }
        LogicalTypeId::Timestamp => {
            Arc::new(primitive::<TimestampMicrosecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Micros, false)
            })?)
        }
        LogicalTypeId::TimestampNs => {
            Arc::new(primitive::<TimestampNanosecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Nanos, false)
            })?)
        }
        LogicalTypeId::TimestampTz => Arc::new(
            primitive::<TimestampMicrosecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Micros, true)
            })?
            .with_timezone(UTC_TIMEZONE),
        ),
        LogicalTypeId::Interval => {
            Arc::new(primitive::<IntervalMonthDayNanoType>(values, |value| {
                as_interval(id, value)
            })?)
        }
        LogicalTypeId::List | LogicalTypeId::Map => {
            build_list(logical_type, list_item(data_type)?, values)?
        }
        LogicalTypeId::Array => {
            let (item, size) = fixed_size_list_item(data_type)?;
            build_fixed_size_list(logical_type, item, size, values)?
        }
        LogicalTypeId::Struct | LogicalTypeId::Variant => {
            build_struct(logical_type, struct_children(data_type)?, values)?
        }
        other => return Err(unsupported(other)),
    })
}

fn build_list(logical_type: &LogicalType, item: &FieldRef, values: &[&Value]) -> Result<ArrayRef> {
    // LIST and MAP share this builder, so the errors name the column's own type.
    let id = logical_type.id;
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut validity = Vec::with_capacity(values.len());
    let mut items = Vec::with_capacity(values.len());
    offsets.push(0i32);
    for value in values {
        match *value {
            Value::Null => validity.push(false),
            Value::List(entries) => {
                items.extend(entries.iter());
                validity.push(true);
            }
            other => return Err(mismatch(id, other)),
        }
        offsets.push(i32::try_from(items.len()).map_err(|_| {
            QuackError::protocol(format!(
                "{id:?} column exceeds the Arrow 32-bit offset limit"
            ))
        })?);
    }
    let child = build_array(get_child_type(logical_type)?, item.data_type(), &items)?;
    Ok(Arc::new(
        ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(offsets.into()),
            child,
            Some(NullBuffer::from(validity)),
        )
        .map_err(arrow_error)?,
    ))
}

fn build_fixed_size_list(
    logical_type: &LogicalType,
    item: &FieldRef,
    size: i32,
    values: &[&Value],
) -> Result<ArrayRef> {
    let width = usize::try_from(size)
        .map_err(|_| QuackError::protocol(format!("Arrow schema declares ARRAY size {size}")))?;
    let mut validity = Vec::with_capacity(values.len());
    let mut items = Vec::with_capacity(values.len() * width);
    for value in values {
        match *value {
            Value::Null => {
                items.extend(std::iter::repeat_n(&NULL_VALUE, width));
                validity.push(false);
            }
            Value::List(entries) => {
                if entries.len() != width {
                    return Err(QuackError::protocol(format!(
                        "ARRAY value has {} entries, expected {width}",
                        entries.len()
                    )));
                }
                items.extend(entries.iter());
                validity.push(true);
            }
            other => return Err(mismatch(logical_type.id, other)),
        }
    }
    let child = build_array(get_child_type(logical_type)?, item.data_type(), &items)?;
    Ok(Arc::new(
        FixedSizeListArray::try_new(item.clone(), size, child, Some(NullBuffer::from(validity)))
            .map_err(arrow_error)?,
    ))
}

fn build_struct(
    logical_type: &LogicalType,
    fields: &Fields,
    values: &[&Value],
) -> Result<ArrayRef> {
    // STRUCT and VARIANT share this builder, so the errors name the column's
    // own type.
    let id = logical_type.id;
    let children = get_struct_children(logical_type)?;
    if children.len() != fields.len() {
        return Err(QuackError::protocol(format!(
            "{id:?} has {} children but the Arrow schema declares {}",
            children.len(),
            fields.len()
        )));
    }
    let mut validity = Vec::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => validity.push(false),
            Value::Struct(row) => {
                // Field names are unique, so matching the arity here and each
                // declared name below together reject a struct carrying an
                // undeclared field.
                if row.len() != children.len() {
                    return Err(QuackError::protocol(format!(
                        "{id:?} value has {} fields but the type declares {}",
                        row.len(),
                        children.len()
                    )));
                }
                validity.push(true);
            }
            other => return Err(mismatch(id, other)),
        }
    }
    let mut arrays = Vec::with_capacity(children.len());
    for (child_index, (child, field)) in zip(children, fields).enumerate() {
        // The decoder inserts struct fields in child order, so the positional
        // lookup hits and the lookup by name is only a fallback. A declared
        // child absent from a present struct is a decoder-invariant violation,
        // not an implicit null.
        let child_values = values
            .iter()
            .map(|value| match *value {
                Value::Struct(row) => match row.get_index(child_index) {
                    Some((name, value)) if name == &child.name => Ok(value),
                    _ => row.get(&child.name).ok_or_else(|| {
                        QuackError::protocol(format!(
                            "{id:?} value has no field named {:?}",
                            child.name
                        ))
                    }),
                },
                // Only a null parent reaches here; the pass above rejected
                // every other variant. Its child slots are null and masked by
                // the parent's validity bit.
                _ => Ok(&NULL_VALUE),
            })
            .collect::<Result<Vec<_>>>()?;
        arrays.push(build_array(
            &child.logical_type,
            field.data_type(),
            &child_values,
        )?);
    }
    Ok(Arc::new(
        StructArray::try_new_with_length(
            fields.clone(),
            arrays,
            Some(NullBuffer::from(validity)),
            values.len(),
        )
        .map_err(arrow_error)?,
    ))
}

fn list_item(data_type: &DataType) -> Result<&FieldRef> {
    match data_type {
        DataType::List(item) => Ok(item),
        other => Err(schema_shape("List", other)),
    }
}

fn fixed_size_list_item(data_type: &DataType) -> Result<(&FieldRef, i32)> {
    match data_type {
        DataType::FixedSizeList(item, size) => Ok((item, *size)),
        other => Err(schema_shape("FixedSizeList", other)),
    }
}

fn struct_children(data_type: &DataType) -> Result<&Fields> {
    match data_type {
        DataType::Struct(fields) => Ok(fields),
        other => Err(schema_shape("Struct", other)),
    }
}

fn primitive<'a, T: ArrowPrimitiveType>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<T::Native>,
) -> Result<PrimitiveArray<T>> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => builder.append_null(),
            value => builder.append_value(extract(value)?),
        }
    }
    Ok(builder.finish())
}

fn boolean<'a>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<bool>,
) -> Result<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => builder.append_null(),
            value => builder.append_value(extract(value)?),
        }
    }
    Ok(builder.finish())
}

fn collect<'a, T>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<T>,
) -> Result<Vec<Option<T>>> {
    values
        .iter()
        .map(|value| match value {
            Value::Null => Ok(None),
            value => extract(value).map(Some),
        })
        .collect()
}

/// Total byte length of a string or blob column, rejecting columns that would
/// overflow Arrow's 32-bit offsets (the builders panic rather than error).
fn byte_capacity<T: AsRef<[u8]>>(id: LogicalTypeId, values: &[Option<T>]) -> Result<usize> {
    let total: usize = values
        .iter()
        .flatten()
        .map(|value| value.as_ref().len())
        .sum();
    if total > i32::MAX as usize {
        return Err(QuackError::protocol(format!(
            "{id:?} column of {total} bytes exceeds the Arrow 32-bit offset limit"
        )));
    }
    Ok(total)
}
