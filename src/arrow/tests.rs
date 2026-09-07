use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::MICROSECONDS_IN_DAY;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, NullArray, StringArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::{IntervalMonthDayNano, i256};
use arrow_schema::{Field, Fields};
use indexmap::IndexMap;

use super::columns::build_array;
use super::schema::arrow_type;
use super::*;
use crate::builders::{column, data_chunk};
use crate::logical_types::ExtraTypeInfo;
use crate::logical_types::LogicalType;
use crate::logical_types::{ChildType, LogicalTypeId, LogicalTypes, logical_type_with_info};
use crate::values::{date_value, decimal_value, interval_value, time_value, timestamp_value};
use crate::vector::{TimeUnit, TimestampUnit};

fn definitions(chunk: &DataChunk) -> Vec<ColumnDefinition> {
    chunk
        .column_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .zip(chunk.types.iter())
        .map(|(name, logical_type)| ColumnDefinition {
            name,
            logical_type: logical_type.clone(),
        })
        .collect()
}

fn record_batch(chunk: &DataChunk) -> RecordBatch {
    let schema = schema(&definitions(chunk)).unwrap();
    to_record_batch(chunk, &schema).unwrap()
}

fn struct_value(entries: Vec<(&str, Value)>) -> Value {
    let mut row = IndexMap::new();
    for (name, value) in entries {
        row.insert(name.to_string(), value);
    }
    Value::Struct(row)
}

fn named(name: &str) -> Option<String> {
    Some(name.to_string())
}

fn enum_type() -> LogicalType {
    LogicalTypes::r#enum(vec![
        "sad".to_string(),
        "ok".to_string(),
        "happy".to_string(),
    ])
}

/// The physical shape DuckDB shreds a `VARIANT` into on the wire, as observed
/// from a live `quack_serve`.
fn variant_type() -> LogicalType {
    logical_type_with_info(
        LogicalTypeId::Variant,
        ExtraTypeInfo::Struct {
            alias: None,
            child_types: vec![
                ChildType {
                    name: "keys".to_string(),
                    logical_type: LogicalTypes::list(LogicalTypes::varchar()),
                },
                ChildType {
                    name: "children".to_string(),
                    logical_type: LogicalTypes::list(LogicalTypes::r#struct(vec![
                        ChildType {
                            name: "keys_index".to_string(),
                            logical_type: LogicalTypes::uinteger(),
                        },
                        ChildType {
                            name: "values_index".to_string(),
                            logical_type: LogicalTypes::uinteger(),
                        },
                    ])),
                },
                ChildType {
                    name: "values".to_string(),
                    logical_type: LogicalTypes::list(LogicalTypes::r#struct(vec![
                        ChildType {
                            name: "type_id".to_string(),
                            logical_type: LogicalTypes::utinyint(),
                        },
                        ChildType {
                            name: "byte_offset".to_string(),
                            logical_type: LogicalTypes::uinteger(),
                        },
                    ])),
                },
                ChildType {
                    name: "data".to_string(),
                    logical_type: LogicalTypes::blob(),
                },
            ],
        },
    )
}

fn point_type() -> LogicalType {
    LogicalTypes::r#struct(vec![
        ChildType {
            name: "x".to_string(),
            logical_type: LogicalTypes::integer(),
        },
        ChildType {
            name: "y".to_string(),
            logical_type: LogicalTypes::varchar(),
        },
    ])
}

/// Every scalar type, as (column name, logical type, one value, the Arrow
/// array expected for that value followed by a null).
fn scalar_cases() -> Vec<(&'static str, LogicalType, Value, ArrayRef)> {
    vec![
        (
            "null_v",
            LogicalTypes::null(),
            Value::Null,
            Arc::new(NullArray::new(2)),
        ),
        (
            "bool_v",
            LogicalTypes::boolean(),
            Value::Bool(true),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ),
        (
            "tiny_v",
            LogicalTypes::tinyint(),
            Value::Int(127),
            Arc::new(Int8Array::from(vec![Some(127), None])),
        ),
        (
            "small_v",
            LogicalTypes::smallint(),
            Value::Int(32767),
            Arc::new(Int16Array::from(vec![Some(32767), None])),
        ),
        (
            "int_v",
            LogicalTypes::integer(),
            Value::Int(2147483647),
            Arc::new(Int32Array::from(vec![Some(2147483647), None])),
        ),
        (
            "big_v",
            LogicalTypes::bigint(),
            Value::Int(9007199254740993),
            Arc::new(Int64Array::from(vec![Some(9007199254740993), None])),
        ),
        (
            "utiny_v",
            LogicalTypes::utinyint(),
            Value::UInt(255),
            Arc::new(UInt8Array::from(vec![Some(255), None])),
        ),
        (
            "usmall_v",
            LogicalTypes::usmallint(),
            Value::UInt(65535),
            Arc::new(UInt16Array::from(vec![Some(65535), None])),
        ),
        (
            "uint_v",
            LogicalTypes::uinteger(),
            Value::UInt(4294967295),
            Arc::new(UInt32Array::from(vec![Some(4294967295), None])),
        ),
        (
            "ubig_v",
            LogicalTypes::ubigint(),
            Value::UInt(u64::MAX),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        ),
        (
            "huge_v",
            LogicalTypes::hugeint(),
            Value::HugeInt(-123456789012345678901234567890),
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(-123456789012345678901234567890)),
                    None,
                ])
                .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                .unwrap(),
            ),
        ),
        (
            "uhuge_v",
            LogicalTypes::uhugeint(),
            Value::UHugeInt(u128::MAX),
            Arc::new(
                Decimal256Array::from(vec![Some(i256::from_parts(u128::MAX, 0)), None])
                    .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                    .unwrap(),
            ),
        ),
        (
            "float_v",
            LogicalTypes::float(),
            Value::Float(1.5),
            Arc::new(Float32Array::from(vec![Some(1.5), None])),
        ),
        (
            "double_v",
            LogicalTypes::double(),
            Value::Double(2.25),
            Arc::new(Float64Array::from(vec![Some(2.25), None])),
        ),
        (
            "dec_v",
            LogicalTypes::decimal(9, 2),
            decimal_value("1234567.89", 9, 2).unwrap(),
            Arc::new(
                Decimal128Array::from(vec![Some(123456789), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
        ),
        (
            "string_v",
            LogicalTypes::varchar(),
            Value::String("hello".to_string()),
            Arc::new(StringArray::from(vec![Some("hello"), None])),
        ),
        (
            "blob_v",
            LogicalTypes::blob(),
            Value::Bytes(b"hi".to_vec()),
            Arc::new(BinaryArray::from(vec![Some(b"hi".as_slice()), None])),
        ),
        (
            "uuid_v",
            LogicalTypes::uuid(),
            Value::String("00112233-4455-6677-8899-aabbccddeeff".to_string()),
            Arc::new(StringArray::from(vec![
                Some("00112233-4455-6677-8899-aabbccddeeff"),
                None,
            ])),
        ),
        (
            "enum_v",
            enum_type(),
            Value::String("ok".to_string()),
            Arc::new(StringArray::from(vec![Some("ok"), None])),
        ),
        (
            "date_v",
            LogicalTypes::date(),
            date_value(18263),
            Arc::new(Date32Array::from(vec![Some(18263), None])),
        ),
        (
            "time_v",
            LogicalTypes::time(),
            time_value(1234567, TimeUnit::Micros),
            Arc::new(Time64MicrosecondArray::from(vec![Some(1234567), None])),
        ),
        (
            "time_ns_v",
            LogicalTypes::time_ns(),
            time_value(1234567890, TimeUnit::Nanos),
            Arc::new(Time64NanosecondArray::from(vec![Some(1234567890), None])),
        ),
        (
            "ts_s_v",
            LogicalTypes::timestamp_seconds(),
            timestamp_value(1, TimestampUnit::Seconds, false),
            Arc::new(TimestampSecondArray::from(vec![Some(1), None])),
        ),
        (
            "ts_ms_v",
            LogicalTypes::timestamp_millis(),
            timestamp_value(1234, TimestampUnit::Millis, false),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1234), None])),
        ),
        (
            "ts_v",
            LogicalTypes::timestamp(),
            timestamp_value(1234567, TimestampUnit::Micros, false),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1234567), None])),
        ),
        (
            "ts_ns_v",
            LogicalTypes::timestamp_nanos(),
            timestamp_value(1234567890, TimestampUnit::Nanos, false),
            Arc::new(TimestampNanosecondArray::from(vec![Some(1234567890), None])),
        ),
        (
            "ts_tz_v",
            LogicalTypes::timestamp_tz(),
            timestamp_value(1234567, TimestampUnit::Micros, true),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1234567), None])
                    .with_timezone(UTC_TIMEZONE),
            ),
        ),
        (
            "interval_v",
            LogicalTypes::interval(),
            interval_value(1, 2, 3),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(1, 2, 3000)),
                None,
            ])),
        ),
    ]
}

#[test]
fn scalar_columns_map_to_their_arrow_types() {
    let cases = scalar_cases();
    let chunk = data_chunk(
        cases
            .iter()
            .map(|(name, logical_type, value, _)| {
                column(
                    logical_type.clone(),
                    [value.clone(), Value::Null],
                    named(name),
                )
            })
            .collect(),
    )
    .unwrap();

    let batch = record_batch(&chunk);

    assert_eq!(batch.num_rows(), 2);
    for (name, _, _, expected) in &cases {
        let index = batch.schema().index_of(name).unwrap();
        assert_eq!(batch.column(index), expected, "column {name}");
    }
}

#[test]
fn build_array_and_arrow_type_agree_for_every_mapped_type() {
    let mut types: Vec<LogicalType> = scalar_cases()
        .into_iter()
        .map(|(_, logical_type, _, _)| logical_type)
        .collect();
    types.extend([
        LogicalTypes::bit(),
        LogicalTypes::geometry(None),
        LogicalTypes::list(LogicalTypes::integer()),
        LogicalTypes::list(point_type()),
        LogicalTypes::array(LogicalTypes::integer(), 2),
        LogicalTypes::map(LogicalTypes::varchar(), LogicalTypes::integer()),
        point_type(),
        variant_type(),
    ]);

    for logical_type in types {
        let declared = arrow_type(&logical_type).unwrap();
        let built = build_array(&logical_type, &declared, &[]).unwrap();
        assert_eq!(
            built.data_type(),
            &declared,
            "{:?} builds an array that disagrees with arrow_type()",
            logical_type.id
        );
    }
}

#[test]
fn nested_columns_preserve_null_containers_and_entries() {
    let chunk = data_chunk(vec![
        column(
            LogicalTypes::list(LogicalTypes::integer()),
            [
                Value::List(vec![Value::Int(1), Value::Null, Value::Int(3)]),
                Value::Null,
                Value::List(vec![]),
            ],
            named("list_v"),
        ),
        column(
            LogicalTypes::array(LogicalTypes::integer(), 2),
            [
                Value::List(vec![Value::Int(7), Value::Int(8)]),
                Value::Null,
                Value::List(vec![Value::Null, Value::Int(9)]),
            ],
            named("array_v"),
        ),
        column(
            point_type(),
            [
                struct_value(vec![
                    ("x", Value::Int(1)),
                    ("y", Value::String("a".to_string())),
                ]),
                Value::Null,
                struct_value(vec![("x", Value::Int(3)), ("y", Value::Null)]),
            ],
            named("struct_v"),
        ),
        column(
            LogicalTypes::map(LogicalTypes::varchar(), LogicalTypes::integer()),
            [
                Value::List(vec![struct_value(vec![
                    ("key", Value::String("a".to_string())),
                    ("value", Value::Int(1)),
                ])]),
                Value::Null,
                Value::List(vec![]),
            ],
            named("map_v"),
        ),
        column(
            LogicalTypes::list(point_type()),
            [
                Value::List(vec![struct_value(vec![
                    ("x", Value::Int(5)),
                    ("y", Value::String("b".to_string())),
                ])]),
                Value::Null,
                Value::List(vec![Value::Null]),
            ],
            named("list_of_struct_v"),
        ),
    ])
    .unwrap();

    let batch = record_batch(&chunk);
    let point_fields = Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);

    assert_eq!(
        batch
            .schema()
            .field_with_name("list_v")
            .unwrap()
            .data_type(),
        &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
    );
    assert_eq!(
        batch
            .schema()
            .field_with_name("array_v")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 2)
    );
    assert_eq!(
        batch.schema().field_with_name("map_v").unwrap().data_type(),
        &DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Int32, true),
            ])),
            true
        )))
    );

    let lists = batch.column_by_name("list_v").unwrap().as_list::<i32>();
    assert!(lists.is_null(1));
    assert_eq!(lists.value_length(0), 3);
    assert_eq!(lists.value_length(2), 0);
    assert_eq!(
        lists.value(0).as_primitive::<Int32Type>(),
        &Int32Array::from(vec![Some(1), None, Some(3)])
    );

    let arrays = batch
        .column_by_name("array_v")
        .unwrap()
        .as_fixed_size_list();
    assert!(arrays.is_null(1));
    assert_eq!(
        arrays.value(2).as_primitive::<Int32Type>(),
        &Int32Array::from(vec![None, Some(9)])
    );

    let structs = batch.column_by_name("struct_v").unwrap().as_struct();
    assert!(structs.is_null(1));
    assert_eq!(
        structs.column(0).as_primitive::<Int32Type>(),
        &Int32Array::from(vec![Some(1), None, Some(3)])
    );
    assert_eq!(
        structs.column(1).as_string::<i32>(),
        &StringArray::from(vec![Some("a"), None, None])
    );

    let list_of_struct = batch
        .column_by_name("list_of_struct_v")
        .unwrap()
        .as_list::<i32>();
    let entries = list_of_struct.value(0);
    let entries = entries.as_struct();
    assert_eq!(entries.fields(), &point_fields);
    assert_eq!(
        entries.column(0).as_primitive::<Int32Type>(),
        &Int32Array::from(vec![Some(5)])
    );
    let null_entry = list_of_struct.value(2);
    assert!(null_entry.is_null(0));
}

#[test]
fn schema_is_built_without_any_chunk() {
    let columns = vec![
        ColumnDefinition {
            name: "id".to_string(),
            logical_type: LogicalTypes::integer(),
        },
        ColumnDefinition {
            name: "label".to_string(),
            logical_type: LogicalTypes::varchar(),
        },
    ];

    let schema = schema(&columns).unwrap();

    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "label"]
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert!(schema.field(0).is_nullable());
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
}

#[test]
fn empty_chunks_keep_the_schema_and_row_count() {
    let chunk = data_chunk(vec![column(LogicalTypes::integer(), [], named("id"))]).unwrap();
    let schema = schema(&[ColumnDefinition {
        name: "id".to_string(),
        logical_type: LogicalTypes::integer(),
    }])
    .unwrap();

    let batch = to_record_batch(&chunk, &schema).unwrap();

    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), schema);
}

#[test]
fn unsupported_types_are_reported_by_name() {
    let error = arrow_type(&LogicalTypes::time_tz()).unwrap_err();

    assert!(
        matches!(&error, QuackError::UnsupportedType(message) if message.contains("TimeTz")),
        "unexpected error: {error}"
    );
}

#[test]
fn times_outside_the_arrow_day_bound_are_rejected() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::time(),
        [time_value(MICROSECONDS_IN_DAY, TimeUnit::Micros)],
        named("time_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("out of range")),
        "unexpected error: {error}"
    );
}

#[test]
fn out_of_range_narrowing_is_a_protocol_error() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::tinyint(),
        [Value::Int(1000)],
        named("tiny_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("out of range")),
        "unexpected error: {error}"
    );
}

#[test]
fn value_variants_that_contradict_the_logical_type_are_rejected() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::integer(),
        [Value::String("nope".to_string())],
        named("int_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("does not match")),
        "unexpected error: {error}"
    );
}

#[test]
fn variant_columns_pass_through_the_struct_duckdb_shreds_them_into() {
    // `SELECT {'x': 1, 'y': 'two'}::VARIANT`, as a live quack_serve decodes it.
    let variant = struct_value(vec![
        (
            "keys",
            Value::List(vec![
                Value::String("x".to_string()),
                Value::String("y".to_string()),
            ]),
        ),
        (
            "children",
            Value::List(vec![
                struct_value(vec![
                    ("keys_index", Value::UInt(0)),
                    ("values_index", Value::UInt(1)),
                ]),
                struct_value(vec![
                    ("keys_index", Value::UInt(1)),
                    ("values_index", Value::UInt(2)),
                ]),
            ]),
        ),
        (
            "values",
            Value::List(vec![
                struct_value(vec![
                    ("type_id", Value::UInt(29)),
                    ("byte_offset", Value::UInt(0)),
                ]),
                struct_value(vec![
                    ("type_id", Value::UInt(5)),
                    ("byte_offset", Value::UInt(2)),
                ]),
                struct_value(vec![
                    ("type_id", Value::UInt(16)),
                    ("byte_offset", Value::UInt(6)),
                ]),
            ]),
        ),
        (
            "data",
            Value::Bytes(vec![2, 0, 1, 0, 0, 0, 3, 116, 119, 111]),
        ),
    ]);
    let chunk = data_chunk(vec![column(
        variant_type(),
        [variant, Value::Null],
        named("variant_v"),
    )])
    .unwrap();

    let batch = record_batch(&chunk);

    let column = batch.column(0).as_struct();
    assert_eq!(column.data_type(), &arrow_type(&variant_type()).unwrap());
    let keys = column.column_by_name("keys").unwrap().as_list::<i32>();
    assert_eq!(
        keys.value(0).as_string::<i32>(),
        &StringArray::from(vec!["x", "y"])
    );
    // A NULL variant is a null container, not a struct of empty children.
    assert!(column.is_null(1));
    assert_eq!(
        column
            .column_by_name("data")
            .unwrap()
            .as_binary::<i32>()
            .value(0),
        &[2, 0, 1, 0, 0, 0, 3, 116, 119, 111]
    );
}

#[test]
fn values_in_a_sqlnull_column_are_rejected() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::null(),
        [Value::Int(7)],
        named("null_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("does not match")),
        "unexpected error: {error}"
    );
}

#[test]
fn structs_missing_a_declared_field_are_rejected() {
    let chunk = data_chunk(vec![column(
        point_type(),
        [struct_value(vec![
            ("x", Value::Int(1)),
            ("z", Value::String("wrong name".to_string())),
        ])],
        named("point_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("no field named \"y\"")),
        "unexpected error: {error}"
    );
}

#[test]
fn structs_carrying_an_undeclared_field_are_rejected() {
    let chunk = data_chunk(vec![column(
        point_type(),
        [struct_value(vec![
            ("x", Value::Int(1)),
            ("y", Value::String("here".to_string())),
            ("z", Value::Int(2)),
        ])],
        named("point_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("has 3 fields but the type declares 2")),
        "unexpected error: {error}"
    );
}

#[test]
fn errors_name_the_column_type_rather_than_the_builder() {
    // VARIANT shares its builder with STRUCT, so the error has to name VARIANT.
    let chunk = data_chunk(vec![column(
        variant_type(),
        [struct_value(vec![
            ("keys", Value::List(vec![])),
            ("data", Value::Bytes(vec![])),
        ])],
        named("variant_v"),
    )])
    .unwrap();
    let schema = schema(&definitions(&chunk)).unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message
            .contains("Variant value has 2 fields but the type declares 4")),
        "unexpected error: {error}"
    );
}

#[test]
fn nested_columns_honour_the_schema_field_names() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::list(LogicalTypes::integer()),
        [Value::List(vec![Value::Int(1), Value::Int(2)])],
        named("list_v"),
    )])
    .unwrap();
    // Parquet and Spark name the list child "element" rather than "item";
    // building against the schema's own field keeps either name working.
    let item = Arc::new(Field::new("element", DataType::Int32, true));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list_v",
        DataType::List(item.clone()),
        true,
    )]));

    let batch = to_record_batch(&chunk, &schema).unwrap();

    assert_eq!(batch.schema(), schema);
    assert_eq!(
        batch.column(0).data_type(),
        &DataType::List(item),
        "the list child field must come from the schema"
    );
}

#[test]
fn nested_columns_whose_schema_shape_disagrees_are_rejected() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::list(LogicalTypes::integer()),
        [Value::List(vec![Value::Int(1)])],
        named("list_v"),
    )])
    .unwrap();
    let schema = schema(&[ColumnDefinition {
        name: "list_v".to_string(),
        logical_type: LogicalTypes::varchar(),
    }])
    .unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("where List is required")),
        "unexpected error: {error}"
    );
}

#[test]
fn chunks_that_disagree_with_the_schema_are_rejected() {
    let chunk = data_chunk(vec![column(
        LogicalTypes::varchar(),
        [Value::String("hello".to_string())],
        named("id"),
    )])
    .unwrap();
    let schema = schema(&[ColumnDefinition {
        name: "id".to_string(),
        logical_type: LogicalTypes::integer(),
    }])
    .unwrap();

    let error = to_record_batch(&chunk, &schema).unwrap_err();

    assert!(
        matches!(&error, QuackError::Protocol(message) if message.contains("column types must match schema types")),
        "unexpected error: {error}"
    );
}
