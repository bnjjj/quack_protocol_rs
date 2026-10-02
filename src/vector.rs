use indexmap::IndexMap;

use crate::binary::{
    BinaryReader, BinaryWriter, HugeIntParts, combine_signed_huge_int, combine_unsigned_huge_int,
    split_signed_huge_int,
};
use crate::constants::OPTIONAL_INDEX_INVALID;
use crate::errors::{QuackError, Result};
use crate::logical_types::{
    ExtraTypeInfo, LogicalType, LogicalTypeId, PhysicalType, decode_logical_type,
    encode_logical_type, get_array_size, get_child_type, get_enum_values, get_physical_type,
    get_struct_children, is_constant_size_physical_type, physical_type_size,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub(crate) enum VectorType {
    Flat = 0,
    Fsst = 1,
    Constant = 2,
    Dictionary = 3,
    Sequence = 4,
}

impl TryFrom<u64> for VectorType {
    type Error = QuackError;

    fn try_from(value: u64) -> Result<Self> {
        Ok(match value {
            0 => Self::Flat,
            1 => Self::Fsst,
            2 => Self::Constant,
            3 => Self::Dictionary,
            4 => Self::Sequence,
            _ => return Err(QuackError::protocol(format!("unknown vector type {value}"))),
        })
    }
}

/// How a string-like (VARCHAR, BLOB, ...) vector is laid out on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StringLayout {
    /// A list of strings (field 102), the layout before DuckDB storage version 2.0.
    #[default]
    List,
    /// DuckDB storage version 2.0 (Quack v3): the byte length of the data (field 107), a
    /// little-endian `u32` length for every row (108), then the bytes of the valid rows (109).
    LengthsAndBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Micros,
    Nanos,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampUnit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecimalValue {
    pub value: i128,
    pub width: u64,
    pub scale: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateValue {
    pub days: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeValue {
    pub unit: TimeUnit,
    pub value: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeTzValue {
    pub bits: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimestampValue {
    pub unit: TimestampUnit,
    pub value: i64,
    pub timezone_utc: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    HugeInt(i128),
    UHugeInt(u128),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal(DecimalValue),
    Date(DateValue),
    Time(TimeValue),
    TimeTz(TimeTzValue),
    Timestamp(TimestampValue),
    Interval(IntervalValue),
    List(Vec<Value>),
    Struct(IndexMap<String, Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::UInt(value as u64)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::UInt(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::Float(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DecodedVector {
    pub(crate) logical_type: LogicalType,
    pub(crate) vector_type: VectorType,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataChunk {
    pub row_count: usize,
    pub types: Vec<LogicalType>,
    pub(crate) columns: Vec<DecodedVector>,
    pub column_names: Option<Vec<String>>,
}

impl DataChunk {
    pub fn column_values(&self, column: usize) -> Option<&[Value]> {
        self.columns
            .get(column)
            .map(|vector| vector.values.as_slice())
    }
}

pub type Row = IndexMap<String, Value>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ListEntry {
    offset: usize,
    length: usize,
}

pub(crate) fn decode_data_chunk_wrapper(reader: &mut BinaryReader<'_>) -> Result<DataChunk> {
    reader.read_object(|object| object.read_required_field(300, decode_data_chunk))
}

pub(crate) fn encode_data_chunk_wrapper(
    writer: &mut BinaryWriter,
    chunk: &DataChunk,
    layout: StringLayout,
) -> Result<()> {
    writer.write_object(|object| {
        object.write_field(300, |object| {
            encode_data_chunk_with_layout(object, chunk, layout)
        })?;
        Ok(())
    })
}

pub fn decode_data_chunk(reader: &mut BinaryReader<'_>) -> Result<DataChunk> {
    reader.read_object(|object| {
        let row_count = object.read_required_field(100, |object| object.read_uleb_usize())?;
        let types = object.read_required_field(101, |object| {
            object.read_list(|object, _| decode_logical_type(object))
        })?;
        let columns = object.read_required_field(102, |object| {
            object.read_list(|object, index| {
                let logical_type = types.get(index).ok_or_else(|| {
                    QuackError::protocol(format!(
                        "column vector {index} has no matching logical type"
                    ))
                })?;
                decode_vector(object, logical_type, row_count)
            })
        })?;
        if columns.len() != types.len() {
            return Err(QuackError::protocol(format!(
                "DataChunk declared {} types but serialized {} columns",
                types.len(),
                columns.len()
            )));
        }
        Ok(DataChunk {
            row_count,
            types,
            columns,
            column_names: None,
        })
    })
}

#[cfg(test)]
pub(crate) fn encode_data_chunk(writer: &mut BinaryWriter, chunk: &DataChunk) -> Result<()> {
    encode_data_chunk_with_layout(writer, chunk, StringLayout::List)
}

/// Writes `chunk` as a DuckDB `DataChunk` object, with string vectors in `layout`.
pub fn encode_data_chunk_with_layout(
    writer: &mut BinaryWriter,
    chunk: &DataChunk,
    layout: StringLayout,
) -> Result<()> {
    if chunk.types.len() != chunk.columns.len() {
        return Err(QuackError::protocol(
            "DataChunk type count must match column count",
        ));
    }
    writer.write_object(|object| {
        object.write_field(100, |object| object.write_uleb(chunk.row_count as u64))?;
        object.write_field(101, |object| {
            object.write_list(&chunk.types, |object, logical_type, _| {
                encode_logical_type(object, logical_type)
            })
        })?;
        object.write_field(102, |object| {
            object.write_list(&chunk.columns, |object, column, index| {
                let logical_type = chunk.types.get(index).ok_or_else(|| {
                    QuackError::protocol(format!("column {index} has no logical type"))
                })?;
                if column.values.len() != chunk.row_count {
                    return Err(QuackError::protocol(format!(
                        "column {index} has {} values, expected {}",
                        column.values.len(),
                        chunk.row_count
                    )));
                }
                encode_vector(
                    object,
                    logical_type,
                    &column.values,
                    chunk.row_count,
                    layout,
                )
            })
        })?;
        Ok(())
    })
}

pub(crate) fn decode_vector(
    reader: &mut BinaryReader<'_>,
    logical_type: &LogicalType,
    count: usize,
) -> Result<DecodedVector> {
    reader.read_object(|object| decode_vector_body(object, logical_type, count))
}

pub(crate) fn encode_vector(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
    values: &[Value],
    count: usize,
    layout: StringLayout,
) -> Result<()> {
    writer
        .write_object(|object| encode_flat_vector_body(object, logical_type, values, count, layout))
}

pub fn rows_from_chunk(chunk: &DataChunk) -> Result<Vec<Row>> {
    let names = chunk.column_names.clone().unwrap_or_else(|| {
        (0..chunk.columns.len())
            .map(|i| format!("column{i}"))
            .collect()
    });
    rows_from_chunk_with_names(chunk, &names)
}

pub(crate) fn rows_from_chunk_with_names(chunk: &DataChunk, names: &[String]) -> Result<Vec<Row>> {
    let mut rows = Vec::with_capacity(chunk.row_count);
    for row_index in 0..chunk.row_count {
        let mut row = Row::new();
        for (column_index, column) in chunk.columns.iter().enumerate() {
            let name = names
                .get(column_index)
                .cloned()
                .unwrap_or_else(|| format!("column{column_index}"));
            let value = column.values.get(row_index).cloned().unwrap_or(Value::Null);
            row.insert(name, value);
        }
        rows.push(row);
    }
    Ok(rows)
}

pub fn decimal_to_string(decimal: &DecimalValue) -> String {
    let negative = decimal.value < 0;
    let abs = if negative {
        -decimal.value
    } else {
        decimal.value
    };
    if decimal.scale == 0 {
        return format!("{}{}", if negative { "-" } else { "" }, abs);
    }
    let factor = 10i128.pow(decimal.scale as u32);
    let integer = abs / factor;
    let fraction = abs % factor;
    format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        integer,
        fraction,
        width = decimal.scale as usize
    )
}

fn decode_vector_body(
    reader: &mut BinaryReader<'_>,
    logical_type: &LogicalType,
    count: usize,
) -> Result<DecodedVector> {
    let vector_type = VectorType::try_from(reader.read_optional_field(
        90,
        |reader| reader.read_uleb_u64(),
        VectorType::Flat as u64,
    )?)?;
    match vector_type {
        VectorType::Flat => decode_flat_vector_body(reader, logical_type, count, vector_type),
        VectorType::Fsst => Err(QuackError::unsupported(
            "FSST-compressed vectors are not supported",
        )),
        VectorType::Constant => {
            let decoded = decode_vector_body(reader, logical_type, usize::from(count > 0))?;
            let value = decoded.values.first().cloned().unwrap_or(Value::Null);
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values: vec![value; count],
            })
        }
        VectorType::Dictionary => {
            let selection =
                reader.read_required_field(91, |reader| read_selection_vector(reader, count))?;
            let dictionary_count =
                reader.read_required_field(92, |reader| reader.read_uleb_usize())?;
            let dictionary = decode_vector_body(reader, logical_type, dictionary_count)?;
            let mut values = Vec::with_capacity(count);
            for index in selection {
                values.push(dictionary.values.get(index).cloned().ok_or_else(|| {
                    QuackError::protocol(format!("dictionary selection {index} is out of range"))
                })?);
            }
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values,
            })
        }
        VectorType::Sequence => {
            let start = reader.read_required_field(91, |reader| reader.read_sleb_i128())?;
            let increment = reader.read_required_field(92, |reader| reader.read_sleb_i128())?;
            let values = (0..count)
                .map(|index| decode_sequence_value(logical_type, start + increment * index as i128))
                .collect::<Result<Vec<_>>>()?;
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values,
            })
        }
    }
}

fn decode_flat_vector_body(
    reader: &mut BinaryReader<'_>,
    logical_type: &LogicalType,
    count: usize,
    vector_type: VectorType,
) -> Result<DecodedVector> {
    if logical_type.id == LogicalTypeId::Geometry && reader.peek_field_id()? == 99 {
        reader.read_required_field(99, |reader| reader.read_uleb_u64())?;
    }

    let has_validity_mask = reader.read_required_field(100, |reader| reader.read_bool())?;
    let validity = if has_validity_mask {
        Some(reader.read_required_field(101, |reader| read_validity_mask(reader, count))?)
    } else {
        None
    };
    let physical_type = get_physical_type(logical_type)?;
    if is_constant_size_physical_type(physical_type) {
        let byte_length = physical_type_size(physical_type)?
            .checked_mul(count)
            .ok_or_else(|| QuackError::protocol(format!("vector of {count} rows is too large")))?;
        let bytes = reader.read_required_field(102, |reader| reader.read_blob())?;
        if bytes.len() != byte_length {
            return Err(QuackError::protocol(format!(
                "fixed-size vector data has {} bytes, expected {byte_length}",
                bytes.len()
            )));
        }
        let values = decode_fixed_values(
            logical_type,
            physical_type,
            &bytes,
            count,
            validity.as_deref(),
        )?;
        return Ok(DecodedVector {
            logical_type: logical_type.clone(),
            vector_type,
            values,
        });
    }

    match physical_type {
        PhysicalType::Varchar => {
            let raw_values = read_string_vector_data(reader, count, validity.as_deref())?;
            let values = raw_values
                .into_iter()
                .enumerate()
                .map(|(index, raw)| {
                    if is_valid(validity.as_deref(), index) {
                        decode_string_like_value(logical_type, raw)
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values,
            })
        }
        PhysicalType::Struct => {
            let children = get_struct_children(logical_type)?;
            let child_vectors = reader.read_required_field(103, |reader| {
                reader.read_list(|reader, index| {
                    let child = children.get(index).ok_or_else(|| {
                        QuackError::protocol(format!(
                            "STRUCT child vector {index} has no matching type metadata"
                        ))
                    })?;
                    decode_vector(reader, &child.logical_type, count)
                })
            })?;
            let mut values = Vec::with_capacity(count);
            for row_index in 0..count {
                if !is_valid(validity.as_deref(), row_index) {
                    values.push(Value::Null);
                    continue;
                }
                let mut row = IndexMap::new();
                for (child_index, child) in children.iter().enumerate() {
                    let child_vector = child_vectors.get(child_index).ok_or_else(|| {
                        QuackError::protocol(format!("STRUCT child {child_index} is incomplete"))
                    })?;
                    row.insert(
                        child.name.clone(),
                        child_vector
                            .values
                            .get(row_index)
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
                values.push(Value::Struct(row));
            }
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values,
            })
        }
        PhysicalType::List => {
            let list_size = reader.read_required_field(104, |reader| reader.read_uleb_usize())?;
            let entries =
                reader.read_required_field(105, |reader| read_list_entries(reader, count))?;
            let child_type = get_child_type(logical_type)?;
            let child_vector = reader
                .read_required_field(106, |reader| decode_vector(reader, child_type, list_size))?;
            let values = entries
                .iter()
                .enumerate()
                .map(|(row_index, entry)| {
                    if !is_valid(validity.as_deref(), row_index) {
                        return Ok(Value::Null);
                    }
                    let items = entry
                        .offset
                        .checked_add(entry.length)
                        .and_then(|end| child_vector.values.get(entry.offset..end))
                        .ok_or_else(|| {
                            QuackError::protocol(format!(
                                "LIST entry {row_index} ({}, {}) is outside its {} child values",
                                entry.offset,
                                entry.length,
                                child_vector.values.len()
                            ))
                        })?;
                    Ok(Value::List(items.to_vec()))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values,
            })
        }
        PhysicalType::Array => {
            let array_size = reader.read_required_field(103, |reader| reader.read_uleb_usize())?;
            let expected = get_array_size(logical_type)? as usize;
            if array_size != expected {
                return Err(QuackError::protocol(format!(
                    "ARRAY vector serialized size {array_size}, expected {expected}"
                )));
            }
            let child_type = get_child_type(logical_type)?;
            let child_count = array_size.checked_mul(count).ok_or_else(|| {
                QuackError::protocol(format!("ARRAY of {count} x {array_size} is too large"))
            })?;
            let child_vector = reader.read_required_field(104, |reader| {
                decode_vector(reader, child_type, child_count)
            })?;
            let values = (0..count)
                .map(|row_index| {
                    if !is_valid(validity.as_deref(), row_index) {
                        return Value::Null;
                    }
                    let offset = row_index * array_size;
                    Value::List(child_vector.values[offset..offset + array_size].to_vec())
                })
                .collect();
            Ok(DecodedVector {
                logical_type: logical_type.clone(),
                vector_type,
                values,
            })
        }
        other => Err(QuackError::unsupported(format!(
            "variable-width physical type {other:?} is not supported"
        ))),
    }
}

fn encode_flat_vector_body(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
    values: &[Value],
    count: usize,
    layout: StringLayout,
) -> Result<()> {
    if values.len() != count {
        return Err(QuackError::protocol(format!(
            "vector value count {} does not match row count {count}",
            values.len()
        )));
    }
    if logical_type.id == LogicalTypeId::Geometry {
        writer.write_field(99, |writer| writer.write_uleb(1u64))?;
    }
    let validity: Vec<bool> = values.iter().map(|value| !value.is_null()).collect();
    let has_validity_mask = validity.iter().any(|valid| !valid);
    writer.write_field(100, |writer| writer.write_bool(has_validity_mask))?;
    if has_validity_mask {
        writer.write_field(101, |writer| {
            writer.write_blob(&write_validity_mask(&validity))
        })?;
    }
    let physical_type = get_physical_type(logical_type)?;
    if is_constant_size_physical_type(physical_type) {
        let mut data = BinaryWriter::with_capacity(physical_type_size(physical_type)? * count);
        for value in values {
            encode_fixed_value(&mut data, logical_type, physical_type, value)?;
        }
        writer.write_field(102, |writer| writer.write_blob(data.as_slice()))?;
        return Ok(());
    }
    match physical_type {
        PhysicalType::Varchar => match layout {
            StringLayout::List => writer.write_field(102, |writer| {
                writer.write_list(values, |writer, value, _| {
                    writer.write_string_bytes(&encode_string_like_value(logical_type, value))
                })
            }),
            StringLayout::LengthsAndBytes => {
                let mut lengths = Vec::with_capacity(values.len() * size_of::<u32>());
                let mut bytes = Vec::new();
                for value in values {
                    let encoded = encode_string_like_value(logical_type, value);
                    let length = u32::try_from(encoded.len()).map_err(|_| {
                        QuackError::protocol("string value is longer than u32::MAX bytes")
                    })?;
                    lengths.extend_from_slice(&length.to_le_bytes());
                    bytes.extend_from_slice(&encoded);
                }
                write_string_vector_data(writer, &lengths, &bytes)
            }
        },
        PhysicalType::Struct => {
            encode_struct_vector_body(writer, logical_type, values, count, layout)
        }
        PhysicalType::List => encode_list_vector_body(writer, logical_type, values, layout),
        PhysicalType::Array => encode_array_vector_body(writer, logical_type, values, layout),
        other => Err(QuackError::unsupported(format!(
            "cannot encode physical type {other:?}"
        ))),
    }
}

/// Reads the payload of a string-like vector.
///
/// DuckDB has two layouts. From storage version 2.0, which the Quack server
/// uses on DuckDB 2.0, a vector carries the byte length of its string data
/// (field 107), a little-endian `u32` length for every row (108), and the bytes
/// of the valid rows back to back (109). Older servers send a list of strings
/// (102). Invalid rows are empty either way; the caller masks them.
fn read_string_vector_data(
    reader: &mut BinaryReader<'_>,
    count: usize,
    validity: Option<&[bool]>,
) -> Result<Vec<Vec<u8>>> {
    let byte_data_length = reader
        .read_optional_field(107, |reader| reader.read_uleb_u64().map(Some), None)?
        .filter(|length| *length != OPTIONAL_INDEX_INVALID);
    let Some(byte_data_length) = byte_data_length else {
        return reader.read_required_field(102, |reader| {
            reader.read_list(|reader, _| reader.read_string_bytes())
        });
    };

    let length_data = reader.read_required_field(108, |reader| reader.read_blob())?;
    let byte_data = reader.read_required_field(109, |reader| reader.read_blob())?;
    if Some(length_data.len()) != count.checked_mul(size_of::<u32>()) {
        return Err(QuackError::protocol(format!(
            "string vector has {} bytes of lengths, expected {} for {count} rows",
            length_data.len(),
            count.saturating_mul(size_of::<u32>())
        )));
    }
    if byte_data.len() as u64 != byte_data_length {
        return Err(QuackError::protocol(format!(
            "string vector has {} bytes of data, header declares {byte_data_length}",
            byte_data.len()
        )));
    }

    let mut offset = 0;
    let mut values = Vec::with_capacity(count);
    for (index, length) in length_data.chunks_exact(size_of::<u32>()).enumerate() {
        if !is_valid(validity, index) {
            values.push(Vec::new());
            continue;
        }
        let length = u32::from_le_bytes([length[0], length[1], length[2], length[3]]) as usize;
        let value = byte_data.get(offset..offset + length).ok_or_else(|| {
            QuackError::protocol(format!(
                "string {index} of {length} bytes overruns the vector's {} bytes of data",
                byte_data.len()
            ))
        })?;
        values.push(value.to_vec());
        offset += length;
    }
    if offset != byte_data.len() {
        return Err(QuackError::protocol(format!(
            "string vector uses {offset} of its {} bytes of data",
            byte_data.len()
        )));
    }
    Ok(values)
}

/// Writes the storage-version-2.0 payload of a string vector: the data length (field 107),
/// the per-row `u32` lengths (108) and the bytes of the valid rows (109). Invalid rows
/// must have length 0.
pub fn write_string_vector_data(
    writer: &mut BinaryWriter,
    lengths: &[u8],
    bytes: &[u8],
) -> Result<()> {
    writer.write_field(107, |writer| writer.write_uleb(bytes.len() as u64))?;
    writer.write_field(108, |writer| writer.write_blob(lengths))?;
    writer.write_field(109, |writer| writer.write_blob(bytes))
}

fn decode_fixed_values(
    logical_type: &LogicalType,
    physical_type: PhysicalType,
    bytes: &[u8],
    count: usize,
    validity: Option<&[bool]>,
) -> Result<Vec<Value>> {
    let mut reader = BinaryReader::new(bytes);
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let value = decode_fixed_value(&mut reader, logical_type, physical_type)?;
        values.push(if is_valid(validity, index) {
            value
        } else {
            Value::Null
        });
    }
    reader.assert_eof()?;
    Ok(values)
}

fn decode_fixed_value(
    reader: &mut BinaryReader<'_>,
    logical_type: &LogicalType,
    physical_type: PhysicalType,
) -> Result<Value> {
    Ok(match physical_type {
        PhysicalType::Bool => Value::Bool(reader.read_fixed_u8()? != 0),
        PhysicalType::Int8 => Value::Int(reader.read_fixed_i8()? as i64),
        PhysicalType::UInt8 => decode_enum_or_number(logical_type, reader.read_fixed_u8()? as u64)?,
        PhysicalType::Int16 => {
            let value = reader.read_fixed_i16()? as i128;
            if logical_type.id == LogicalTypeId::Decimal {
                Value::Decimal(decimal_from_unscaled(logical_type, value)?)
            } else {
                Value::Int(value as i64)
            }
        }
        PhysicalType::UInt16 => {
            decode_enum_or_number(logical_type, reader.read_fixed_u16()? as u64)?
        }
        PhysicalType::Int32 => {
            let value = reader.read_fixed_i32()?;
            if logical_type.id == LogicalTypeId::Date {
                Value::Date(DateValue { days: value })
            } else if logical_type.id == LogicalTypeId::Decimal {
                Value::Decimal(decimal_from_unscaled(logical_type, value as i128)?)
            } else {
                Value::Int(value as i64)
            }
        }
        PhysicalType::UInt32 => {
            decode_enum_or_number(logical_type, reader.read_fixed_u32()? as u64)?
        }
        PhysicalType::Int64 => decode_int64_logical_value(logical_type, reader.read_fixed_i64()?)?,
        PhysicalType::UInt64 => Value::UInt(reader.read_fixed_u64()?),
        PhysicalType::Float => Value::Float(reader.read_fixed_f32()?),
        PhysicalType::Double => Value::Double(reader.read_fixed_f64()?),
        PhysicalType::Int128 => {
            let lower = reader.read_fixed_u64()?;
            let upper = reader.read_fixed_i64()?;
            if logical_type.id == LogicalTypeId::Uuid {
                Value::String(HugeIntParts { upper, lower }.to_string())
            } else {
                let value = combine_signed_huge_int(HugeIntParts { upper, lower });
                if logical_type.id == LogicalTypeId::Decimal {
                    Value::Decimal(decimal_from_unscaled(logical_type, value)?)
                } else {
                    Value::HugeInt(value)
                }
            }
        }
        PhysicalType::UInt128 => {
            let lower = reader.read_fixed_u64()?;
            let upper = reader.read_fixed_u64()? as i64;
            Value::UHugeInt(combine_unsigned_huge_int(HugeIntParts { upper, lower }))
        }
        PhysicalType::Interval => Value::Interval(IntervalValue {
            months: reader.read_fixed_i32()?,
            days: reader.read_fixed_i32()?,
            micros: reader.read_fixed_i64()?,
        }),
        other => {
            return Err(QuackError::unsupported(format!(
                "cannot decode fixed physical type {other:?}"
            )));
        }
    })
}

fn encode_fixed_value(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
    physical_type: PhysicalType,
    value: &Value,
) -> Result<()> {
    if value.is_null() {
        return write_null_value(writer, physical_type);
    }
    match physical_type {
        PhysicalType::Bool => writer.write_fixed_u8(if matches!(value, Value::Bool(true)) {
            1
        } else {
            0
        }),
        PhysicalType::Int8 => writer.write_fixed_i8(value_to_i128(value)? as i8),
        PhysicalType::UInt8 => {
            writer.write_fixed_u8(encode_enum_or_number(logical_type, value)? as u8)
        }
        PhysicalType::Int16 => {
            writer.write_fixed_i16(encode_decimal_or_integer(logical_type, value)? as i16)
        }
        PhysicalType::UInt16 => {
            writer.write_fixed_u16(encode_enum_or_number(logical_type, value)? as u16)
        }
        PhysicalType::Int32 => {
            writer.write_fixed_i32(encode_date_decimal_or_integer(logical_type, value)? as i32)
        }
        PhysicalType::UInt32 => {
            writer.write_fixed_u32(encode_enum_or_number(logical_type, value)? as u32)
        }
        PhysicalType::Int64 => {
            writer.write_fixed_i64(encode_int64_logical_value(logical_type, value)?)
        }
        PhysicalType::UInt64 => writer.write_fixed_u64(value_to_u128(value)? as u64),
        PhysicalType::Float => writer.write_fixed_f32(value_to_f64(value)? as f32),
        PhysicalType::Double => writer.write_fixed_f64(value_to_f64(value)?),
        PhysicalType::Int128 => {
            let parts = if logical_type.id == LogicalTypeId::Uuid {
                match value {
                    Value::String(uuid) => uuid_to_huge_int_parts(uuid)?,
                    _ => return Err(QuackError::protocol("UUID values must be strings")),
                }
            } else {
                split_signed_huge_int(encode_int128_logical_value(logical_type, value)?)
            };
            writer.write_fixed_u64(parts.lower)?;
            writer.write_fixed_i64(parts.upper)
        }
        PhysicalType::UInt128 => {
            let value = value_to_u128(value)?;
            writer.write_fixed_u64(value as u64)?;
            writer.write_fixed_u64((value >> 64) as u64)
        }
        PhysicalType::Interval => {
            let interval = match value {
                Value::Interval(interval) => *interval,
                _ => {
                    return Err(QuackError::protocol(
                        "INTERVAL values must be tagged intervals",
                    ));
                }
            };
            writer.write_fixed_i32(interval.months)?;
            writer.write_fixed_i32(interval.days)?;
            writer.write_fixed_i64(interval.micros)
        }
        other => Err(QuackError::unsupported(format!(
            "cannot encode fixed physical type {other:?}"
        ))),
    }
}

/// Writes what DuckDB stores in a NULL slot (`NullValue<T>()`): the minimum of a
/// signed type, 0 of an unsigned one, NaN for floats, each field's minimum for an
/// INTERVAL.
fn write_null_value(writer: &mut BinaryWriter, physical_type: PhysicalType) -> Result<()> {
    match physical_type {
        PhysicalType::Bool | PhysicalType::Int8 => writer.write_fixed_i8(i8::MIN),
        PhysicalType::Int16 => writer.write_fixed_i16(i16::MIN),
        PhysicalType::Int32 => writer.write_fixed_i32(i32::MIN),
        PhysicalType::Int64 => writer.write_fixed_i64(i64::MIN),
        PhysicalType::Int128 => {
            writer.write_fixed_u64(0)?;
            writer.write_fixed_i64(i64::MIN)
        }
        PhysicalType::Float => writer.write_fixed_f32(f32::NAN),
        PhysicalType::Double => writer.write_fixed_f64(f64::NAN),
        PhysicalType::Interval => {
            writer.write_fixed_i32(i32::MIN)?;
            writer.write_fixed_i32(i32::MIN)?;
            writer.write_fixed_i64(i64::MIN)
        }
        other => writer.write_bytes(&vec![0; physical_type_size(other)?]),
    }
}

fn decode_string_like_value(logical_type: &LogicalType, raw: Vec<u8>) -> Result<Value> {
    match logical_type.id {
        LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit => Ok(Value::Bytes(raw)),
        _ => Ok(Value::String(String::from_utf8(raw)?)),
    }
}

fn encode_string_like_value(logical_type: &LogicalType, value: &Value) -> Vec<u8> {
    if value.is_null() {
        return Vec::new();
    }
    match (logical_type.id, value) {
        (
            LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit,
            Value::Bytes(bytes),
        ) => bytes.clone(),
        (_, Value::String(value)) => value.as_bytes().to_vec(),
        (_, Value::Bytes(bytes)) => bytes.clone(),
        _ => value_to_string_lossy(value).into_bytes(),
    }
}

fn encode_struct_vector_body(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
    values: &[Value],
    count: usize,
    layout: StringLayout,
) -> Result<()> {
    let children = get_struct_children(logical_type)?;
    writer.write_field(103, |writer| {
        writer.write_list(children, |writer, child, _| {
            let child_values = values
                .iter()
                .map(|value| match value {
                    Value::Struct(row) => row.get(&child.name).cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                })
                .collect::<Vec<_>>();
            encode_vector(writer, &child.logical_type, &child_values, count, layout)
        })
    })
}

fn encode_list_vector_body(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
    values: &[Value],
    layout: StringLayout,
) -> Result<()> {
    let child_type = get_child_type(logical_type)?;
    let mut entries = Vec::with_capacity(values.len());
    let mut child_values = Vec::new();
    for value in values {
        match value {
            Value::Null => entries.push(ListEntry {
                offset: 0,
                length: 0,
            }),
            Value::List(items) => {
                let offset = child_values.len();
                child_values.extend(items.iter().cloned());
                entries.push(ListEntry {
                    offset,
                    length: items.len(),
                });
            }
            _ => return Err(QuackError::protocol("LIST/MAP values must be lists")),
        }
    }
    writer.write_field(104, |writer| writer.write_uleb(child_values.len() as u64))?;
    writer.write_field(105, |writer| write_list_entries(writer, &entries))?;
    writer.write_field(106, |writer| {
        encode_vector(
            writer,
            child_type,
            &child_values,
            child_values.len(),
            layout,
        )
    })
}

fn encode_array_vector_body(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
    values: &[Value],
    layout: StringLayout,
) -> Result<()> {
    let child_type = get_child_type(logical_type)?;
    let array_size = get_array_size(logical_type)? as usize;
    let mut child_values = Vec::with_capacity(values.len() * array_size);
    for value in values {
        match value {
            Value::Null => child_values.extend(std::iter::repeat_n(Value::Null, array_size)),
            Value::List(items) if items.len() == array_size => {
                child_values.extend(items.iter().cloned());
            }
            Value::List(_) => {
                return Err(QuackError::protocol(format!(
                    "ARRAY values must be lists of length {array_size}"
                )));
            }
            _ => return Err(QuackError::protocol("ARRAY values must be lists")),
        }
    }
    writer.write_field(103, |writer| writer.write_uleb(array_size as u64))?;
    writer.write_field(104, |writer| {
        encode_vector(
            writer,
            child_type,
            &child_values,
            child_values.len(),
            layout,
        )
    })
}

fn read_selection_vector(reader: &mut BinaryReader<'_>, count: usize) -> Result<Vec<usize>> {
    let expected_bytes = count
        .checked_mul(4)
        .ok_or_else(|| QuackError::protocol("selection vector is too large"))?;
    let bytes = reader.read_blob()?;
    if bytes.len() != expected_bytes {
        return Err(QuackError::protocol(format!(
            "selection vector has {} bytes, expected {expected_bytes}",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().expect("chunk size")) as usize)
        .collect())
}

fn read_validity_mask(reader: &mut BinaryReader<'_>, count: usize) -> Result<Vec<bool>> {
    let expected_bytes = validity_mask_size(count);
    let bytes = reader.read_blob()?;
    if bytes.len() != expected_bytes {
        return Err(QuackError::protocol(format!(
            "validity mask has {} bytes, expected {expected_bytes}",
            bytes.len()
        )));
    }
    Ok((0..count)
        .map(|index| {
            let byte = bytes.get(index / 8).copied().unwrap_or(0);
            (byte & (1 << (index % 8))) != 0
        })
        .collect())
}

/// Packs row validity into DuckDB's mask: one bit per row, in 64-bit words.
pub fn write_validity_mask(validity: &[bool]) -> Vec<u8> {
    // as DuckDB's ValidityMask: all valid, padding included, then the NULLs cleared
    let mut bytes = vec![0xffu8; validity_mask_size(validity.len())];
    for (index, valid) in validity.iter().enumerate() {
        if !*valid {
            bytes[index / 8] &= !(1 << (index % 8));
        }
    }
    bytes
}

/// Bytes in a DuckDB validity mask for `count` rows.
pub fn validity_mask_size(count: usize) -> usize {
    count.div_ceil(64) * 8
}

fn is_valid(validity: Option<&[bool]>, index: usize) -> bool {
    validity.map(|values| values[index]).unwrap_or(true)
}

fn read_list_entries(reader: &mut BinaryReader<'_>, count: usize) -> Result<Vec<ListEntry>> {
    let entries = reader.read_list(|reader, _| {
        reader.read_object(|object| {
            Ok(ListEntry {
                offset: object.read_required_field(100, |object| object.read_uleb_usize())?,
                length: object.read_required_field(101, |object| object.read_uleb_usize())?,
            })
        })
    })?;
    if entries.len() != count {
        return Err(QuackError::protocol(format!(
            "LIST vector serialized {} entries for {count} rows",
            entries.len()
        )));
    }
    Ok(entries)
}

fn write_list_entries(writer: &mut BinaryWriter, entries: &[ListEntry]) -> Result<()> {
    writer.write_list(entries, |writer, entry, _| {
        writer.write_object(|object| {
            object.write_field(100, |object| object.write_uleb(entry.offset as u64))?;
            object.write_field(101, |object| object.write_uleb(entry.length as u64))?;
            Ok(())
        })
    })
}

fn decode_enum_or_number(logical_type: &LogicalType, index: u64) -> Result<Value> {
    if logical_type.id != LogicalTypeId::Enum {
        return Ok(Value::UInt(index));
    }
    let value = get_enum_values(logical_type)?
        .get(index as usize)
        .ok_or_else(|| QuackError::protocol(format!("ENUM index {index} is out of range")))?;
    Ok(Value::String(value.clone()))
}

fn encode_enum_or_number(logical_type: &LogicalType, value: &Value) -> Result<u64> {
    if logical_type.id != LogicalTypeId::Enum {
        return value_to_u128(value).map(|value| value as u64);
    }
    match value {
        Value::UInt(value) => Ok(*value),
        Value::Int(value) if *value >= 0 => Ok(*value as u64),
        Value::String(value) => get_enum_values(logical_type)?
            .iter()
            .position(|candidate| candidate == value)
            .map(|index| index as u64)
            .ok_or_else(|| QuackError::protocol(format!("unknown ENUM value {value}"))),
        _ => Err(QuackError::protocol(
            "ENUM values must be strings or integers",
        )),
    }
}

fn decode_int64_logical_value(logical_type: &LogicalType, value: i64) -> Result<Value> {
    Ok(match logical_type.id {
        LogicalTypeId::Time => Value::Time(TimeValue {
            unit: TimeUnit::Micros,
            value,
        }),
        LogicalTypeId::TimeNs => Value::Time(TimeValue {
            unit: TimeUnit::Nanos,
            value,
        }),
        LogicalTypeId::TimeTz => Value::TimeTz(TimeTzValue { bits: value }),
        LogicalTypeId::TimestampSec => Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Seconds,
            value,
            timezone_utc: false,
        }),
        LogicalTypeId::TimestampMs => Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Millis,
            value,
            timezone_utc: false,
        }),
        LogicalTypeId::Timestamp => Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Micros,
            value,
            timezone_utc: false,
        }),
        LogicalTypeId::TimestampNs => Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Nanos,
            value,
            timezone_utc: false,
        }),
        LogicalTypeId::TimestampTz => Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Micros,
            value,
            timezone_utc: true,
        }),
        LogicalTypeId::Decimal => {
            Value::Decimal(decimal_from_unscaled(logical_type, value as i128)?)
        }
        _ => Value::Int(value),
    })
}

fn encode_int64_logical_value(logical_type: &LogicalType, value: &Value) -> Result<i64> {
    if logical_type.id == LogicalTypeId::Decimal {
        return Ok(decimal_to_unscaled(logical_type, value)? as i64);
    }
    match value {
        Value::Time(value) => Ok(value.value),
        Value::TimeTz(value) => Ok(value.bits),
        Value::Timestamp(value) => Ok(value.value),
        _ => Ok(value_to_i128(value)? as i64),
    }
}

fn encode_int128_logical_value(logical_type: &LogicalType, value: &Value) -> Result<i128> {
    if logical_type.id == LogicalTypeId::Decimal {
        decimal_to_unscaled(logical_type, value)
    } else {
        value_to_i128(value)
    }
}

fn encode_decimal_or_integer(logical_type: &LogicalType, value: &Value) -> Result<i128> {
    if logical_type.id == LogicalTypeId::Decimal {
        decimal_to_unscaled(logical_type, value)
    } else {
        value_to_i128(value)
    }
}

fn encode_date_decimal_or_integer(logical_type: &LogicalType, value: &Value) -> Result<i128> {
    if logical_type.id == LogicalTypeId::Date {
        return match value {
            Value::Date(value) => Ok(value.days as i128),
            _ => value_to_i128(value),
        };
    }
    encode_decimal_or_integer(logical_type, value)
}

fn decimal_from_unscaled(logical_type: &LogicalType, value: i128) -> Result<DecimalValue> {
    match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Decimal { width, scale, .. }) => Ok(DecimalValue {
            value,
            width: *width,
            scale: *scale,
        }),
        _ => Err(QuackError::protocol(
            "DECIMAL value is missing DecimalTypeInfo",
        )),
    }
}

pub(crate) fn decimal_to_unscaled(logical_type: &LogicalType, value: &Value) -> Result<i128> {
    match value {
        Value::Decimal(value) => Ok(value.value),
        Value::String(value) => parse_decimal_string(logical_type, value),
        _ => value_to_i128(value),
    }
}

fn parse_decimal_string(logical_type: &LogicalType, value: &str) -> Result<i128> {
    let scale = match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Decimal { scale, .. }) => *scale as usize,
        _ => {
            return Err(QuackError::protocol(
                "DECIMAL value is missing DecimalTypeInfo",
            ));
        }
    };
    let trimmed = value.trim();
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix(['-', '+']).unwrap_or(trimmed);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut padded_fraction = fraction.to_string();
    while padded_fraction.len() < scale {
        padded_fraction.push('0');
    }
    padded_fraction.truncate(scale);
    let text = format!(
        "{}{}",
        if integer.is_empty() { "0" } else { integer },
        padded_fraction
    );
    let unscaled = text
        .parse::<i128>()
        .map_err(|_| QuackError::protocol(format!("invalid decimal value {value}")))?;
    Ok(if negative { -unscaled } else { unscaled })
}

fn value_to_i128(value: &Value) -> Result<i128> {
    match value {
        Value::Bool(value) => Ok(i128::from(*value)),
        Value::Int(value) => Ok(*value as i128),
        Value::UInt(value) => Ok(*value as i128),
        Value::HugeInt(value) => Ok(*value),
        Value::UHugeInt(value) => i128::try_from(*value)
            .map_err(|_| QuackError::protocol("unsigned integer does not fit i128")),
        Value::String(value) => value
            .parse::<i128>()
            .map_err(|_| QuackError::protocol(format!("cannot convert {value} to integer"))),
        _ => Err(QuackError::protocol(format!(
            "cannot convert {value:?} to integer"
        ))),
    }
}

fn value_to_u128(value: &Value) -> Result<u128> {
    match value {
        Value::Bool(value) => Ok(u128::from(*value)),
        Value::Int(value) if *value >= 0 => Ok(*value as u128),
        Value::UInt(value) => Ok(*value as u128),
        Value::HugeInt(value) if *value >= 0 => Ok(*value as u128),
        Value::UHugeInt(value) => Ok(*value),
        Value::String(value) => value.parse::<u128>().map_err(|_| {
            QuackError::protocol(format!("cannot convert {value} to unsigned integer"))
        }),
        _ => Err(QuackError::protocol(format!(
            "cannot convert {value:?} to unsigned integer"
        ))),
    }
}

fn value_to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Float(value) => Ok(*value as f64),
        Value::Double(value) => Ok(*value),
        Value::Int(value) => Ok(*value as f64),
        Value::UInt(value) => Ok(*value as f64),
        Value::String(value) => value
            .parse::<f64>()
            .map_err(|_| QuackError::protocol(format!("cannot convert {value} to float"))),
        _ => Err(QuackError::protocol(format!(
            "cannot convert {value:?} to float"
        ))),
    }
}

fn decode_sequence_value(logical_type: &LogicalType, value: i128) -> Result<Value> {
    match logical_type.id {
        LogicalTypeId::Integer => Ok(Value::Int(value as i64)),
        LogicalTypeId::Date => Ok(Value::Date(DateValue { days: value as i32 })),
        LogicalTypeId::BigInt => Ok(Value::Int(value as i64)),
        _ => decode_int64_logical_value(logical_type, value as i64),
    }
}

fn uuid_to_huge_int_parts(uuid: &str) -> Result<HugeIntParts> {
    let hex = uuid.replace('-', "").to_ascii_lowercase();
    if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(QuackError::protocol(format!("invalid UUID string {uuid}")));
    }
    let display_upper = u64::from_str_radix(&hex[0..16], 16)
        .map_err(|_| QuackError::protocol(format!("invalid UUID string {uuid}")))?;
    let lower = u64::from_str_radix(&hex[16..32], 16)
        .map_err(|_| QuackError::protocol(format!("invalid UUID string {uuid}")))?;
    let upper = (display_upper ^ (1u64 << 63)) as i64;
    Ok(HugeIntParts { upper, lower })
}

fn value_to_string_lossy(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::HugeInt(value) => value.to_string(),
        Value::UHugeInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        Value::Decimal(value) => decimal_to_string(value),
        Value::Date(value) => value.days.to_string(),
        Value::Time(value) => value.value.to_string(),
        Value::TimeTz(value) => value.bits.to_string(),
        Value::Timestamp(value) => value.value.to_string(),
        Value::Interval(value) => format!("{} {} {}", value.months, value.days, value.micros),
        Value::List(_) | Value::Struct(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_types::LogicalTypes;

    /// A flat string vector in DuckDB's storage version 2.0 layout.
    fn v2_string_vector(values: &[Option<&[u8]>], byte_data_length: Option<u64>) -> Vec<u8> {
        let validity: Vec<bool> = values.iter().map(Option::is_some).collect();
        let lengths: Vec<u8> = values
            .iter()
            .flat_map(|value| (value.map_or(0, <[u8]>::len) as u32).to_le_bytes())
            .collect();
        let bytes: Vec<u8> = values.iter().flatten().flat_map(|v| v.to_vec()).collect();
        let mut writer = BinaryWriter::new();
        writer
            .write_object(|object| {
                let has_validity_mask = validity.iter().any(|valid| !valid);
                object.write_field(100, |object| object.write_bool(has_validity_mask))?;
                if has_validity_mask {
                    object.write_field(101, |object| {
                        object.write_blob(&write_validity_mask(&validity))
                    })?;
                }
                object.write_field(107, |object| {
                    object.write_uleb(byte_data_length.unwrap_or(bytes.len() as u64))
                })?;
                object.write_field(108, |object| object.write_blob(&lengths))?;
                object.write_field(109, |object| object.write_blob(&bytes))
            })
            .unwrap();
        writer.into_bytes()
    }

    fn decode(bytes: &[u8], logical_type: &LogicalType, count: usize) -> Result<Vec<Value>> {
        let mut reader = BinaryReader::new(bytes);
        let vector = decode_vector(&mut reader, logical_type, count)?;
        reader.assert_eof()?;
        Ok(vector.values)
    }

    #[test]
    fn decodes_v2_string_vector_with_nulls_and_empty_strings() {
        let bytes = v2_string_vector(&[Some(b"ab"), None, Some(b""), Some(b"cde")], None);
        assert_eq!(
            decode(&bytes, &LogicalTypes::varchar(), 4).unwrap(),
            vec![
                Value::String("ab".into()),
                Value::Null,
                Value::String(String::new()),
                Value::String("cde".into()),
            ]
        );
    }

    #[test]
    fn decodes_v2_blob_vector_as_bytes() {
        let bytes = v2_string_vector(&[Some(&[0, 159, 146, 150])], None);
        assert_eq!(
            decode(&bytes, &LogicalTypes::blob(), 1).unwrap(),
            vec![Value::Bytes(vec![0, 159, 146, 150])]
        );
    }

    #[test]
    fn still_decodes_list_string_vector() {
        let values = vec![Value::String("x".into()), Value::Null];
        let mut writer = BinaryWriter::new();
        encode_vector(
            &mut writer,
            &LogicalTypes::varchar(),
            &values,
            2,
            StringLayout::List,
        )
        .unwrap();
        assert_eq!(
            decode(&writer.into_bytes(), &LogicalTypes::varchar(), 2).unwrap(),
            values
        );
    }

    #[test]
    fn hostile_list_entries_and_counts_are_errors_not_panics() {
        // a LIST vector whose entry points past its one child value
        let mut writer = BinaryWriter::new();
        writer
            .write_object(|object| {
                object.write_field(100, |object| object.write_bool(false))?;
                object.write_field(104, |object| object.write_uleb(1u64))?;
                object.write_field(105, |object| {
                    write_list_entries(
                        object,
                        &[ListEntry {
                            offset: 0,
                            length: 5,
                        }],
                    )
                })?;
                object.write_field(106, |object| {
                    encode_vector(
                        object,
                        &LogicalTypes::integer(),
                        &[Value::Int(1)],
                        1,
                        StringLayout::List,
                    )
                })
            })
            .unwrap();
        let list = LogicalTypes::list(LogicalTypes::integer());
        assert!(decode(&writer.into_bytes(), &list, 1).is_err());

        // a string that claims u64::MAX bytes
        let mut writer = BinaryWriter::new();
        writer.write_uleb(u64::MAX).unwrap();
        assert!(BinaryReader::new(writer.as_slice()).read_string().is_err());

        // a list that claims 2^60 elements
        let mut writer = BinaryWriter::new();
        writer.write_uleb(1u64 << 60).unwrap();
        let mut reader = BinaryReader::new(writer.as_slice());
        assert!(reader.read_list(|reader, _| reader.read_byte()).is_err());

        // a fixed-size vector of usize::MAX rows
        let mut writer = BinaryWriter::new();
        writer
            .write_object(|object| {
                object.write_field(100, |object| object.write_bool(false))?;
                object.write_field(102, |object| object.write_blob(&[0; 8]))
            })
            .unwrap();
        assert!(decode(&writer.into_bytes(), &LogicalTypes::bigint(), usize::MAX).is_err());
    }

    #[test]
    fn rejects_v2_string_vector_whose_lengths_disagree_with_its_data() {
        let bytes = v2_string_vector(&[Some(b"ab")], Some(3));
        assert!(decode(&bytes, &LogicalTypes::varchar(), 1).is_err());
        // Two rows declared, one row of lengths sent.
        let bytes = v2_string_vector(&[Some(b"ab")], None);
        assert!(decode(&bytes, &LogicalTypes::varchar(), 2).is_err());
    }
}
