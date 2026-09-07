use crate::binary::{BinaryReader, BinaryWriter};
use crate::errors::{QuackError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub(crate) enum PhysicalType {
    Bool = 1,
    UInt8 = 2,
    Int8 = 3,
    UInt16 = 4,
    Int16 = 5,
    UInt32 = 6,
    Int32 = 7,
    UInt64 = 8,
    Int64 = 9,
    Float = 11,
    Double = 12,
    Interval = 21,
    List = 23,
    Struct = 24,
    Array = 29,
    Varchar = 200,
    UInt128 = 203,
    Int128 = 204,
    Bit = 206,
    Invalid = 255,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum LogicalTypeId {
    Invalid = 0,
    SqlNull = 1,
    Unknown = 2,
    Any = 3,
    Unbound = 4,
    Template = 5,
    Type = 6,
    Boolean = 10,
    TinyInt = 11,
    SmallInt = 12,
    Integer = 13,
    BigInt = 14,
    Date = 15,
    Time = 16,
    TimestampSec = 17,
    TimestampMs = 18,
    Timestamp = 19,
    TimestampNs = 20,
    Decimal = 21,
    Float = 22,
    Double = 23,
    Char = 24,
    Varchar = 25,
    Blob = 26,
    Interval = 27,
    UTinyInt = 28,
    USmallInt = 29,
    UInteger = 30,
    UBigInt = 31,
    TimestampTz = 32,
    TimeTz = 34,
    TimeNs = 35,
    Bit = 36,
    StringLiteral = 37,
    IntegerLiteral = 38,
    BigNum = 39,
    UHugeInt = 49,
    HugeInt = 50,
    Pointer = 51,
    Validity = 53,
    Uuid = 54,
    Geometry = 60,
    Struct = 100,
    List = 101,
    Map = 102,
    Table = 103,
    Enum = 104,
    AggregateState = 105,
    Lambda = 106,
    Union = 107,
    Array = 108,
    Variant = 109,
}

impl TryFrom<u64> for LogicalTypeId {
    type Error = QuackError;

    fn try_from(value: u64) -> Result<Self> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::SqlNull,
            2 => Self::Unknown,
            3 => Self::Any,
            4 => Self::Unbound,
            5 => Self::Template,
            6 => Self::Type,
            10 => Self::Boolean,
            11 => Self::TinyInt,
            12 => Self::SmallInt,
            13 => Self::Integer,
            14 => Self::BigInt,
            15 => Self::Date,
            16 => Self::Time,
            17 => Self::TimestampSec,
            18 => Self::TimestampMs,
            19 => Self::Timestamp,
            20 => Self::TimestampNs,
            21 => Self::Decimal,
            22 => Self::Float,
            23 => Self::Double,
            24 => Self::Char,
            25 => Self::Varchar,
            26 => Self::Blob,
            27 => Self::Interval,
            28 => Self::UTinyInt,
            29 => Self::USmallInt,
            30 => Self::UInteger,
            31 => Self::UBigInt,
            32 => Self::TimestampTz,
            34 => Self::TimeTz,
            35 => Self::TimeNs,
            36 => Self::Bit,
            37 => Self::StringLiteral,
            38 => Self::IntegerLiteral,
            39 => Self::BigNum,
            49 => Self::UHugeInt,
            50 => Self::HugeInt,
            51 => Self::Pointer,
            53 => Self::Validity,
            54 => Self::Uuid,
            60 => Self::Geometry,
            100 => Self::Struct,
            101 => Self::List,
            102 => Self::Map,
            103 => Self::Table,
            104 => Self::Enum,
            105 => Self::AggregateState,
            106 => Self::Lambda,
            107 => Self::Union,
            108 => Self::Array,
            109 => Self::Variant,
            _ => {
                return Err(QuackError::protocol(format!(
                    "unknown LogicalTypeId {value}"
                )));
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub(crate) enum ExtraTypeInfoType {
    Invalid = 0,
    Generic = 1,
    Decimal = 2,
    String = 3,
    List = 4,
    Struct = 5,
    Enum = 6,
    Unbound = 7,
    AggregateState = 8,
    Array = 9,
    Any = 10,
    IntegerLiteral = 11,
    Template = 12,
    Geo = 13,
}

impl TryFrom<u64> for ExtraTypeInfoType {
    type Error = QuackError;

    fn try_from(value: u64) -> Result<Self> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::Generic,
            2 => Self::Decimal,
            3 => Self::String,
            4 => Self::List,
            5 => Self::Struct,
            6 => Self::Enum,
            7 => Self::Unbound,
            8 => Self::AggregateState,
            9 => Self::Array,
            10 => Self::Any,
            11 => Self::IntegerLiteral,
            12 => Self::Template,
            13 => Self::Geo,
            _ => {
                return Err(QuackError::protocol(format!(
                    "unknown ExtraTypeInfoType {value}"
                )));
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalType {
    pub id: LogicalTypeId,
    pub type_info: Option<ExtraTypeInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildType {
    pub name: String,
    pub logical_type: LogicalType,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct CoordinateReferenceSystem {
    pub definition: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExtraTypeInfo {
    Invalid {
        alias: Option<String>,
    },
    Generic {
        alias: Option<String>,
    },
    Decimal {
        alias: Option<String>,
        width: u64,
        scale: u64,
    },
    String {
        alias: Option<String>,
        collation: String,
    },
    List {
        alias: Option<String>,
        child_type: Box<LogicalType>,
    },
    Struct {
        alias: Option<String>,
        child_types: Vec<ChildType>,
    },
    Enum {
        alias: Option<String>,
        values: Vec<String>,
    },
    AggregateState {
        alias: Option<String>,
        function_name: String,
        return_type: Box<LogicalType>,
        bound_argument_types: Vec<LogicalType>,
    },
    Array {
        alias: Option<String>,
        child_type: Box<LogicalType>,
        size: u64,
    },
    Any {
        alias: Option<String>,
        target_type: Box<LogicalType>,
        cast_score: u64,
    },
    Template {
        alias: Option<String>,
        name: String,
    },
    IntegerLiteral {
        alias: Option<String>,
    },
    Geo {
        alias: Option<String>,
        crs: CoordinateReferenceSystem,
    },
    Unbound {
        alias: Option<String>,
        name: Option<String>,
        catalog: Option<String>,
        schema: Option<String>,
    },
}

impl ExtraTypeInfo {
    pub(crate) fn type_id(&self) -> ExtraTypeInfoType {
        match self {
            Self::Invalid { .. } => ExtraTypeInfoType::Invalid,
            Self::Generic { .. } => ExtraTypeInfoType::Generic,
            Self::Decimal { .. } => ExtraTypeInfoType::Decimal,
            Self::String { .. } => ExtraTypeInfoType::String,
            Self::List { .. } => ExtraTypeInfoType::List,
            Self::Struct { .. } => ExtraTypeInfoType::Struct,
            Self::Enum { .. } => ExtraTypeInfoType::Enum,
            Self::AggregateState { .. } => ExtraTypeInfoType::AggregateState,
            Self::Array { .. } => ExtraTypeInfoType::Array,
            Self::Any { .. } => ExtraTypeInfoType::Any,
            Self::Template { .. } => ExtraTypeInfoType::Template,
            Self::IntegerLiteral { .. } => ExtraTypeInfoType::IntegerLiteral,
            Self::Geo { .. } => ExtraTypeInfoType::Geo,
            Self::Unbound { .. } => ExtraTypeInfoType::Unbound,
        }
    }

    pub fn alias(&self) -> Option<&str> {
        match self {
            Self::Invalid { alias }
            | Self::Generic { alias }
            | Self::Decimal { alias, .. }
            | Self::String { alias, .. }
            | Self::List { alias, .. }
            | Self::Struct { alias, .. }
            | Self::Enum { alias, .. }
            | Self::AggregateState { alias, .. }
            | Self::Array { alias, .. }
            | Self::Any { alias, .. }
            | Self::Template { alias, .. }
            | Self::IntegerLiteral { alias }
            | Self::Geo { alias, .. }
            | Self::Unbound { alias, .. } => alias.as_deref(),
        }
    }
}

pub(crate) fn logical_type(id: LogicalTypeId) -> LogicalType {
    LogicalType {
        id,
        type_info: None,
    }
}

pub(crate) fn logical_type_with_info(id: LogicalTypeId, type_info: ExtraTypeInfo) -> LogicalType {
    LogicalType {
        id,
        type_info: Some(type_info),
    }
}

pub struct LogicalTypes;

impl LogicalTypes {
    pub fn null() -> LogicalType {
        logical_type(LogicalTypeId::SqlNull)
    }
    pub fn boolean() -> LogicalType {
        logical_type(LogicalTypeId::Boolean)
    }
    pub fn tinyint() -> LogicalType {
        logical_type(LogicalTypeId::TinyInt)
    }
    pub fn smallint() -> LogicalType {
        logical_type(LogicalTypeId::SmallInt)
    }
    pub fn integer() -> LogicalType {
        logical_type(LogicalTypeId::Integer)
    }
    pub fn bigint() -> LogicalType {
        logical_type(LogicalTypeId::BigInt)
    }
    pub fn utinyint() -> LogicalType {
        logical_type(LogicalTypeId::UTinyInt)
    }
    pub fn usmallint() -> LogicalType {
        logical_type(LogicalTypeId::USmallInt)
    }
    pub fn uinteger() -> LogicalType {
        logical_type(LogicalTypeId::UInteger)
    }
    pub fn ubigint() -> LogicalType {
        logical_type(LogicalTypeId::UBigInt)
    }
    pub fn hugeint() -> LogicalType {
        logical_type(LogicalTypeId::HugeInt)
    }
    pub fn uhugeint() -> LogicalType {
        logical_type(LogicalTypeId::UHugeInt)
    }
    pub fn float() -> LogicalType {
        logical_type(LogicalTypeId::Float)
    }
    pub fn double() -> LogicalType {
        logical_type(LogicalTypeId::Double)
    }
    pub fn varchar() -> LogicalType {
        logical_type(LogicalTypeId::Varchar)
    }
    pub fn blob() -> LogicalType {
        logical_type(LogicalTypeId::Blob)
    }
    pub fn bit() -> LogicalType {
        logical_type(LogicalTypeId::Bit)
    }
    pub fn uuid() -> LogicalType {
        logical_type(LogicalTypeId::Uuid)
    }
    pub fn date() -> LogicalType {
        logical_type(LogicalTypeId::Date)
    }
    pub fn time() -> LogicalType {
        logical_type(LogicalTypeId::Time)
    }
    pub fn time_ns() -> LogicalType {
        logical_type(LogicalTypeId::TimeNs)
    }
    pub fn time_tz() -> LogicalType {
        logical_type(LogicalTypeId::TimeTz)
    }
    pub fn timestamp() -> LogicalType {
        logical_type(LogicalTypeId::Timestamp)
    }
    pub fn timestamp_seconds() -> LogicalType {
        logical_type(LogicalTypeId::TimestampSec)
    }
    pub fn timestamp_millis() -> LogicalType {
        logical_type(LogicalTypeId::TimestampMs)
    }
    pub fn timestamp_nanos() -> LogicalType {
        logical_type(LogicalTypeId::TimestampNs)
    }
    pub fn timestamp_tz() -> LogicalType {
        logical_type(LogicalTypeId::TimestampTz)
    }
    pub fn interval() -> LogicalType {
        logical_type(LogicalTypeId::Interval)
    }

    pub fn decimal(width: u64, scale: u64) -> LogicalType {
        logical_type_with_info(
            LogicalTypeId::Decimal,
            ExtraTypeInfo::Decimal {
                alias: None,
                width,
                scale,
            },
        )
    }

    pub fn list(child_type: LogicalType) -> LogicalType {
        logical_type_with_info(
            LogicalTypeId::List,
            ExtraTypeInfo::List {
                alias: None,
                child_type: Box::new(child_type),
            },
        )
    }

    pub fn map(key_type: LogicalType, value_type: LogicalType) -> LogicalType {
        Self::list(Self::r#struct(vec![
            ChildType {
                name: "key".to_string(),
                logical_type: key_type,
            },
            ChildType {
                name: "value".to_string(),
                logical_type: value_type,
            },
        ]))
        .with_id(LogicalTypeId::Map)
    }

    pub fn r#struct(child_types: Vec<ChildType>) -> LogicalType {
        logical_type_with_info(
            LogicalTypeId::Struct,
            ExtraTypeInfo::Struct {
                alias: None,
                child_types,
            },
        )
    }

    pub fn array(child_type: LogicalType, size: u64) -> LogicalType {
        logical_type_with_info(
            LogicalTypeId::Array,
            ExtraTypeInfo::Array {
                alias: None,
                child_type: Box::new(child_type),
                size,
            },
        )
    }

    pub fn r#enum(values: Vec<String>) -> LogicalType {
        logical_type_with_info(
            LogicalTypeId::Enum,
            ExtraTypeInfo::Enum {
                alias: None,
                values,
            },
        )
    }

    pub fn geometry(crs: Option<CoordinateReferenceSystem>) -> LogicalType {
        match crs {
            Some(crs) => logical_type_with_info(
                LogicalTypeId::Geometry,
                ExtraTypeInfo::Geo { alias: None, crs },
            ),
            None => logical_type(LogicalTypeId::Geometry),
        }
    }
}

impl LogicalType {
    fn with_id(mut self, id: LogicalTypeId) -> Self {
        self.id = id;
        self
    }
}

pub(crate) fn encode_logical_type(
    writer: &mut BinaryWriter,
    logical_type: &LogicalType,
) -> Result<()> {
    writer.write_object(|object| {
        object.write_field(100, |object| object.write_uleb(logical_type.id as u64))?;
        if let Some(info) = &logical_type.type_info {
            object.write_field(101, |object| {
                object.write_nullable(Some(info), encode_extra_type_info)
            })?;
        }
        Ok(())
    })
}

pub(crate) fn decode_logical_type(reader: &mut BinaryReader<'_>) -> Result<LogicalType> {
    reader.read_object(|object| {
        let id = LogicalTypeId::try_from(
            object.read_required_field(100, |object| object.read_uleb_u64())?,
        )?;
        let type_info = object.read_optional_field(
            101,
            |object| object.read_nullable(decode_extra_type_info),
            None,
        )?;
        Ok(LogicalType { id, type_info })
    })
}

pub(crate) fn encode_extra_type_info(
    writer: &mut BinaryWriter,
    info: &ExtraTypeInfo,
) -> Result<()> {
    writer.write_object(|object| {
        object.write_field(100, |object| object.write_uleb(info.type_id() as u64))?;
        if let Some(alias) = info.alias().filter(|alias| !alias.is_empty()) {
            object.write_field(101, |object| object.write_string(alias))?;
        }
        match info {
            ExtraTypeInfo::Invalid { .. } | ExtraTypeInfo::Generic { .. } => {}
            ExtraTypeInfo::Decimal { width, scale, .. } => {
                object.write_field(200, |object| object.write_uleb(*width))?;
                object.write_field(201, |object| object.write_uleb(*scale))?;
            }
            ExtraTypeInfo::String { collation, .. } => {
                object.write_field(200, |object| object.write_string(collation))?;
            }
            ExtraTypeInfo::List { child_type, .. } => {
                object.write_field(200, |object| encode_logical_type(object, child_type))?;
            }
            ExtraTypeInfo::Struct { child_types, .. } => {
                object.write_field(200, |object| encode_child_types(object, child_types))?;
            }
            ExtraTypeInfo::Enum { values, .. } => {
                object.write_field(200, |object| object.write_uleb(values.len() as u64))?;
                object.write_field(201, |object| {
                    object.write_list(values, |object, value, _| object.write_string(value))
                })?;
            }
            ExtraTypeInfo::AggregateState {
                function_name,
                return_type,
                bound_argument_types,
                ..
            } => {
                object.write_field(200, |object| object.write_string(function_name))?;
                object.write_field(201, |object| encode_logical_type(object, return_type))?;
                object.write_field(202, |object| {
                    object.write_list(bound_argument_types, |object, logical_type, _| {
                        encode_logical_type(object, logical_type)
                    })
                })?;
            }
            ExtraTypeInfo::Array {
                child_type, size, ..
            } => {
                object.write_field(200, |object| encode_logical_type(object, child_type))?;
                object.write_field(201, |object| object.write_uleb(*size))?;
            }
            ExtraTypeInfo::Any {
                target_type,
                cast_score,
                ..
            } => {
                object.write_field(200, |object| encode_logical_type(object, target_type))?;
                object.write_field(201, |object| object.write_uleb(*cast_score))?;
            }
            ExtraTypeInfo::Template { name, .. } => {
                object.write_field(200, |object| object.write_string(name))?;
            }
            ExtraTypeInfo::IntegerLiteral { .. } => {
                return Err(QuackError::unsupported(
                    "encoding INTEGER_LITERAL type metadata is not supported",
                ));
            }
            ExtraTypeInfo::Geo { crs, .. } => {
                object.write_field(200, |object| {
                    encode_coordinate_reference_system(object, crs)
                })?;
            }
            ExtraTypeInfo::Unbound {
                name,
                catalog,
                schema,
                ..
            } => {
                if let Some(name) = name {
                    object.write_field(200, |object| object.write_string(name))?;
                }
                if let Some(catalog) = catalog {
                    object.write_field(201, |object| object.write_string(catalog))?;
                }
                if let Some(schema) = schema {
                    object.write_field(202, |object| object.write_string(schema))?;
                }
            }
        }
        Ok(())
    })
}

pub(crate) fn decode_extra_type_info(reader: &mut BinaryReader<'_>) -> Result<ExtraTypeInfo> {
    reader.read_object(|object| {
        let type_id = ExtraTypeInfoType::try_from(object.read_required_field(100, |object| object.read_uleb_u64())?)?;
        let alias = object.read_optional_field(
            101,
            |object| Ok(Some(object.read_string()?)),
            None::<String>,
        )?;
        if object.peek_field_id()? == 103 {
            return Err(QuackError::unsupported(
                "extension type metadata is not supported",
            ));
        }
        Ok(match type_id {
            ExtraTypeInfoType::Invalid => ExtraTypeInfo::Invalid { alias },
            ExtraTypeInfoType::Generic => ExtraTypeInfo::Generic { alias },
            ExtraTypeInfoType::Decimal => ExtraTypeInfo::Decimal {
                alias,
                width: object.read_optional_field(200, |object| object.read_uleb_u64(), 0)?,
                scale: object.read_optional_field(201, |object| object.read_uleb_u64(), 0)?,
            },
            ExtraTypeInfoType::String => ExtraTypeInfo::String {
                alias,
                collation: object.read_optional_field(200, |object| object.read_string(), String::new())?,
            },
            ExtraTypeInfoType::List => ExtraTypeInfo::List {
                alias,
                child_type: Box::new(object.read_required_field(200, decode_logical_type)?),
            },
            ExtraTypeInfoType::Struct => ExtraTypeInfo::Struct {
                alias,
                child_types: object.read_optional_field(200, decode_child_types, Vec::new())?,
            },
            ExtraTypeInfoType::Enum => {
                let values_count = object.read_required_field(200, |object| object.read_uleb_usize())?;
                let values = object.read_required_field(201, |object| {
                    object.read_list(|object, _| object.read_string())
                })?;
                if values.len() != values_count {
                    return Err(QuackError::protocol(format!(
                        "ENUM metadata declared {values_count} values but serialized {}",
                        values.len()
                    )));
                }
                ExtraTypeInfo::Enum { alias, values }
            }
            ExtraTypeInfoType::AggregateState => ExtraTypeInfo::AggregateState {
                alias,
                function_name: object.read_required_field(200, |object| object.read_string())?,
                return_type: Box::new(object.read_required_field(201, decode_logical_type)?),
                bound_argument_types: object.read_required_field(202, |object| {
                    object.read_list(|object, _| decode_logical_type(object))
                })?,
            },
            ExtraTypeInfoType::Array => ExtraTypeInfo::Array {
                alias,
                child_type: Box::new(object.read_required_field(200, decode_logical_type)?),
                size: object.read_required_field(201, |object| object.read_uleb_u64())?,
            },
            ExtraTypeInfoType::Any => ExtraTypeInfo::Any {
                alias,
                target_type: Box::new(object.read_required_field(200, decode_logical_type)?),
                cast_score: object.read_required_field(201, |object| object.read_uleb_u64())?,
            },
            ExtraTypeInfoType::IntegerLiteral => {
                if object.peek_field_id()? == 200 {
                    return Err(QuackError::unsupported(
                        "INTEGER_LITERAL type metadata contains a DuckDB Value, which is not supported",
                    ));
                }
                ExtraTypeInfo::IntegerLiteral { alias }
            }
            ExtraTypeInfoType::Template => ExtraTypeInfo::Template {
                alias,
                name: object.read_optional_field(200, |object| object.read_string(), String::new())?,
            },
            ExtraTypeInfoType::Geo => ExtraTypeInfo::Geo {
                alias,
                crs: object.read_optional_field(
                    200,
                    decode_coordinate_reference_system,
                    CoordinateReferenceSystem::default(),
                )?,
            },
            ExtraTypeInfoType::Unbound => {
                if object.peek_field_id()? == 204 {
                    return Err(QuackError::unsupported(
                        "UNBOUND type metadata contains a ParsedExpression, which is not supported",
                    ));
                }
                ExtraTypeInfo::Unbound {
                    alias,
                    name: object.read_optional_field(
                        200,
                        |object| Ok(Some(object.read_string()?)),
                        None::<String>,
                    )?,
                    catalog: object.read_optional_field(
                        201,
                        |object| Ok(Some(object.read_string()?)),
                        None::<String>,
                    )?,
                    schema: object.read_optional_field(
                        202,
                        |object| Ok(Some(object.read_string()?)),
                        None::<String>,
                    )?,
                }
            }
        })
    })
}

pub(crate) fn get_physical_type(logical_type: &LogicalType) -> Result<PhysicalType> {
    Ok(match logical_type.id {
        LogicalTypeId::Boolean => PhysicalType::Bool,
        LogicalTypeId::TinyInt => PhysicalType::Int8,
        LogicalTypeId::UTinyInt => PhysicalType::UInt8,
        LogicalTypeId::SmallInt => PhysicalType::Int16,
        LogicalTypeId::USmallInt => PhysicalType::UInt16,
        LogicalTypeId::SqlNull | LogicalTypeId::Date | LogicalTypeId::Integer => {
            PhysicalType::Int32
        }
        LogicalTypeId::UInteger => PhysicalType::UInt32,
        LogicalTypeId::BigInt
        | LogicalTypeId::Time
        | LogicalTypeId::TimeNs
        | LogicalTypeId::Timestamp
        | LogicalTypeId::TimestampSec
        | LogicalTypeId::TimestampNs
        | LogicalTypeId::TimestampMs
        | LogicalTypeId::TimeTz
        | LogicalTypeId::TimestampTz => PhysicalType::Int64,
        LogicalTypeId::UBigInt => PhysicalType::UInt64,
        LogicalTypeId::UHugeInt => PhysicalType::UInt128,
        LogicalTypeId::HugeInt | LogicalTypeId::Uuid => PhysicalType::Int128,
        LogicalTypeId::Float => PhysicalType::Float,
        LogicalTypeId::Double => PhysicalType::Double,
        LogicalTypeId::Decimal => decimal_physical_type(logical_type)?,
        LogicalTypeId::BigNum
        | LogicalTypeId::Varchar
        | LogicalTypeId::Char
        | LogicalTypeId::Blob
        | LogicalTypeId::Bit
        | LogicalTypeId::Type
        | LogicalTypeId::AggregateState
        | LogicalTypeId::Geometry => PhysicalType::Varchar,
        LogicalTypeId::Interval => PhysicalType::Interval,
        LogicalTypeId::Union | LogicalTypeId::Variant | LogicalTypeId::Struct => {
            PhysicalType::Struct
        }
        LogicalTypeId::List | LogicalTypeId::Map => PhysicalType::List,
        LogicalTypeId::Array => PhysicalType::Array,
        LogicalTypeId::Pointer => PhysicalType::UInt64,
        LogicalTypeId::Validity => PhysicalType::Bit,
        LogicalTypeId::Enum => enum_physical_type(logical_type)?,
        _ => PhysicalType::Invalid,
    })
}

pub(crate) fn is_constant_size_physical_type(physical_type: PhysicalType) -> bool {
    matches!(
        physical_type,
        PhysicalType::Bool
            | PhysicalType::UInt8
            | PhysicalType::Int8
            | PhysicalType::UInt16
            | PhysicalType::Int16
            | PhysicalType::UInt32
            | PhysicalType::Int32
            | PhysicalType::UInt64
            | PhysicalType::Int64
            | PhysicalType::Float
            | PhysicalType::Double
            | PhysicalType::Interval
            | PhysicalType::UInt128
            | PhysicalType::Int128
    )
}

pub(crate) fn physical_type_size(physical_type: PhysicalType) -> Result<usize> {
    Ok(match physical_type {
        PhysicalType::Bool | PhysicalType::UInt8 | PhysicalType::Int8 => 1,
        PhysicalType::UInt16 | PhysicalType::Int16 => 2,
        PhysicalType::UInt32 | PhysicalType::Int32 | PhysicalType::Float => 4,
        PhysicalType::UInt64 | PhysicalType::Int64 | PhysicalType::Double => 8,
        PhysicalType::Interval | PhysicalType::UInt128 | PhysicalType::Int128 => 16,
        _ => {
            return Err(QuackError::protocol(format!(
                "physical type {physical_type:?} is not fixed size"
            )));
        }
    })
}

pub(crate) fn get_child_type(logical_type: &LogicalType) -> Result<&LogicalType> {
    match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::List { child_type, .. })
        | Some(ExtraTypeInfo::Array { child_type, .. }) => Ok(child_type),
        _ => Err(QuackError::protocol(format!(
            "logical type {:?} does not have a child type",
            logical_type.id
        ))),
    }
}

pub(crate) fn get_struct_children(logical_type: &LogicalType) -> Result<&[ChildType]> {
    match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Struct { child_types, .. }) => Ok(child_types),
        _ if matches!(
            logical_type.id,
            LogicalTypeId::Variant | LogicalTypeId::Union
        ) =>
        {
            Ok(&[])
        }
        _ => Err(QuackError::protocol(format!(
            "logical type {:?} does not have struct children",
            logical_type.id
        ))),
    }
}

pub(crate) fn get_array_size(logical_type: &LogicalType) -> Result<u64> {
    match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Array { size, .. }) => Ok(*size),
        _ => Err(QuackError::protocol(format!(
            "logical type {:?} is not an ARRAY",
            logical_type.id
        ))),
    }
}

pub(crate) fn get_decimal_width_and_scale(logical_type: &LogicalType) -> Result<(u64, u64)> {
    match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Decimal { width, scale, .. }) => Ok((*width, *scale)),
        _ => Err(QuackError::protocol(
            "DECIMAL type is missing DecimalTypeInfo",
        )),
    }
}

pub(crate) fn get_enum_values(logical_type: &LogicalType) -> Result<&[String]> {
    match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Enum { values, .. }) => Ok(values),
        _ => Err(QuackError::protocol(format!(
            "logical type {:?} is not an ENUM",
            logical_type.id
        ))),
    }
}

fn encode_child_types(writer: &mut BinaryWriter, children: &[ChildType]) -> Result<()> {
    writer.write_list(children, |writer, child, _| {
        writer.write_object(|pair| {
            pair.write_field(0, |pair| pair.write_string(&child.name))?;
            pair.write_field(1, |pair| encode_logical_type(pair, &child.logical_type))?;
            Ok(())
        })
    })
}

fn decode_child_types(reader: &mut BinaryReader<'_>) -> Result<Vec<ChildType>> {
    reader.read_list(|reader, _| {
        reader.read_object(|pair| {
            Ok(ChildType {
                name: pair.read_required_field(0, |pair| pair.read_string())?,
                logical_type: pair.read_required_field(1, decode_logical_type)?,
            })
        })
    })
}

fn encode_coordinate_reference_system(
    writer: &mut BinaryWriter,
    crs: &CoordinateReferenceSystem,
) -> Result<()> {
    writer.write_object(|object| {
        if let Some(definition) = crs.definition.as_deref() {
            object.write_field(100, |object| object.write_string(definition))?;
        }
        Ok(())
    })
}

fn decode_coordinate_reference_system(
    reader: &mut BinaryReader<'_>,
) -> Result<CoordinateReferenceSystem> {
    reader.read_object(|object| {
        Ok(CoordinateReferenceSystem {
            definition: object.read_optional_field(
                100,
                |object| Ok(Some(object.read_string()?)),
                None::<String>,
            )?,
        })
    })
}

fn decimal_physical_type(logical_type: &LogicalType) -> Result<PhysicalType> {
    let (width, _) = get_decimal_width_and_scale(logical_type)?;
    Ok(if width <= 4 {
        PhysicalType::Int16
    } else if width <= 9 {
        PhysicalType::Int32
    } else if width <= 18 {
        PhysicalType::Int64
    } else if width <= 38 {
        PhysicalType::Int128
    } else {
        return Err(QuackError::protocol(format!(
            "unsupported DECIMAL width {width}"
        )));
    })
}

fn enum_physical_type(logical_type: &LogicalType) -> Result<PhysicalType> {
    let value_count = get_enum_values(logical_type)?.len();
    Ok(if value_count <= 0xff {
        PhysicalType::UInt8
    } else if value_count <= 0xffff {
        PhysicalType::UInt16
    } else {
        PhysicalType::UInt32
    })
}
