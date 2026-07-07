use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::TryStreamExt;
use indexmap::IndexMap;
use quack_protocol::*;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

async fn live_client() -> Result<Option<QuackClient>> {
    let uri = match std::env::var("QUACK_SERVER_URI") {
        Ok(uri) => uri,
        Err(_) => return Ok(None),
    };
    let client = QuackClient::connect(
        &uri,
        QuackClientOptions {
            auth_token: std::env::var("QUACK_AUTH_TOKEN").ok(),
            ..Default::default()
        },
    )
    .await?;
    Ok(Some(client))
}

fn unique_name(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}", std::process::id(), id)
}

fn row(entries: Vec<(&str, Value)>) -> Row {
    let mut row = Row::new();
    for (key, value) in entries {
        row.insert(key.to_string(), value);
    }
    row
}

fn struct_value(entries: Vec<(&str, Value)>) -> Value {
    Value::Struct(row(entries))
}

fn assert_decimal(value: &Value, unscaled: i128, width: u64, scale: u64) {
    match value {
        Value::Decimal(value) => {
            assert_eq!(value.value, unscaled);
            assert_eq!(value.width, width);
            assert_eq!(value.scale, scale);
        }
        other => panic!("expected decimal, got {other:?}"),
    }
}

#[tokio::test]
async fn live_quack_basic_query_when_configured() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    let result = client
        .query(
            "
            SELECT *
            FROM (
              VALUES
                (1::INTEGER, 'one'::VARCHAR),
                (2::INTEGER, 'two'::VARCHAR)
            ) AS t(id, label)
            ORDER BY id
            ",
            None,
        )
        .await?;
    let (columns, rows) = result.into_rows();
    let rows: Vec<Row> = rows.try_collect().await?;

    assert_eq!(
        columns,
        vec![
            ColumnDefinition {
                name: "id".to_string(),
                logical_type: LogicalTypes::integer(),
            },
            ColumnDefinition {
                name: "label".to_string(),
                logical_type: LogicalTypes::varchar(),
            },
        ]
    );
    assert_eq!(
        rows,
        vec![
            row(vec![
                ("id", Value::Int(1)),
                ("label", Value::String("one".to_string())),
            ]),
            row(vec![
                ("id", Value::Int(2)),
                ("label", Value::String("two".to_string())),
            ]),
        ]
    );

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_preserves_empty_result_schema() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    let result = client
        .query(
            "SELECT 1::INTEGER AS id, 'x'::VARCHAR AS label WHERE FALSE",
            None,
        )
        .await?;
    let (columns, rows) = result.into_rows();
    let rows: Vec<Row> = rows.try_collect().await?;

    assert_eq!(
        columns,
        vec![
            ColumnDefinition {
                name: "id".to_string(),
                logical_type: LogicalTypes::integer(),
            },
            ColumnDefinition {
                name: "label".to_string(),
                logical_type: LogicalTypes::varchar(),
            },
        ]
    );
    assert!(rows.is_empty());

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_round_trips_scalar_types() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };
    let enum_name = unique_name("quack_rust_mood");
    // The stream is lazy: DDL only executes once the stream is polled.
    let (_, rows) = client
        .query(
            &format!("CREATE TYPE {enum_name} AS ENUM ('sad', 'ok', 'happy')"),
            None,
        )
        .await?
        .into_rows();
    let _: Vec<Row> = rows.try_collect().await?;

    let result = client
        .query(
            &format!(
                "
            SELECT
              TRUE AS bool_v,
              127::TINYINT AS tiny_v,
              32767::SMALLINT AS small_v,
              2147483647::INTEGER AS int_v,
              9007199254740993::BIGINT AS big_v,
              255::UTINYINT AS utiny_v,
              65535::USMALLINT AS usmall_v,
              4294967295::UINTEGER AS uint_v,
              18446744073709551615::UBIGINT AS ubig_v,
              123456789012345678901234567890::HUGEINT AS huge_v,
              123456789012345678901234567890::UHUGEINT AS uhuge_v,
              1.5::FLOAT AS float_v,
              2.25::DOUBLE AS double_v,
              12.34::DECIMAL(4, 2) AS dec16_v,
              1234567.89::DECIMAL(9, 2) AS dec32_v,
              1234567890123456.78::DECIMAL(18, 2) AS dec64_v,
              123456789012345678901234567890.1234::DECIMAL(38, 4) AS dec128_v,
              'hello'::VARCHAR AS string_v,
              'hi'::BLOB AS blob_v,
              '00112233-4455-6677-8899-aabbccddeeff'::UUID AS uuid_v,
              DATE '2020-01-02' AS date_v,
              '00:00:01.234567'::TIME AS time_v,
              '00:00:01.234567890'::TIME_NS AS time_ns_v,
              TIMESTAMP '1970-01-01 00:00:01.234567' AS ts_v,
              '1970-01-01 00:00:01'::TIMESTAMP_S AS ts_s_v,
              '1970-01-01 00:00:01.234'::TIMESTAMP_MS AS ts_ms_v,
              '1970-01-01 00:00:01.234567890'::TIMESTAMP_NS AS ts_ns_v,
              TIMESTAMPTZ '1970-01-01 00:00:01.234567+00' AS ts_tz_v,
              INTERVAL '1 month 2 days 3 microseconds' AS interval_v,
              'ok'::{enum_name} AS enum_v
            "
            ),
            None,
        )
        .await?;
    let (columns, rows) = result.into_rows();
    let types: Vec<LogicalTypeId> = columns
        .iter()
        .map(|column| column.logical_type.id)
        .collect();
    let rows: Vec<Row> = rows.try_collect().await?;

    assert_eq!(
        types,
        vec![
            LogicalTypeId::Boolean,
            LogicalTypeId::TinyInt,
            LogicalTypeId::SmallInt,
            LogicalTypeId::Integer,
            LogicalTypeId::BigInt,
            LogicalTypeId::UTinyInt,
            LogicalTypeId::USmallInt,
            LogicalTypeId::UInteger,
            LogicalTypeId::UBigInt,
            LogicalTypeId::HugeInt,
            LogicalTypeId::UHugeInt,
            LogicalTypeId::Float,
            LogicalTypeId::Double,
            LogicalTypeId::Decimal,
            LogicalTypeId::Decimal,
            LogicalTypeId::Decimal,
            LogicalTypeId::Decimal,
            LogicalTypeId::Varchar,
            LogicalTypeId::Blob,
            LogicalTypeId::Uuid,
            LogicalTypeId::Date,
            LogicalTypeId::Time,
            LogicalTypeId::TimeNs,
            LogicalTypeId::Timestamp,
            LogicalTypeId::TimestampSec,
            LogicalTypeId::TimestampMs,
            LogicalTypeId::TimestampNs,
            LogicalTypeId::TimestampTz,
            LogicalTypeId::Interval,
            LogicalTypeId::Enum,
        ]
    );

    let row = &rows[0];
    assert_eq!(row["bool_v"], Value::Bool(true));
    assert_eq!(row["tiny_v"], Value::Int(127));
    assert_eq!(row["small_v"], Value::Int(32767));
    assert_eq!(row["int_v"], Value::Int(2147483647));
    assert_eq!(row["big_v"], Value::Int(9007199254740993));
    assert_eq!(row["utiny_v"], Value::UInt(255));
    assert_eq!(row["usmall_v"], Value::UInt(65535));
    assert_eq!(row["uint_v"], Value::UInt(4294967295));
    assert_eq!(row["ubig_v"], Value::UInt(u64::MAX));
    assert_eq!(
        row["huge_v"],
        Value::HugeInt(123456789012345678901234567890)
    );
    assert_eq!(
        row["uhuge_v"],
        Value::UHugeInt(123456789012345678901234567890)
    );
    assert_eq!(row["float_v"], Value::Float(1.5));
    assert_eq!(row["double_v"], Value::Double(2.25));
    assert_decimal(&row["dec16_v"], 1234, 4, 2);
    assert_decimal(&row["dec32_v"], 123456789, 9, 2);
    assert_decimal(&row["dec64_v"], 123456789012345678, 18, 2);
    assert_decimal(&row["dec128_v"], 1234567890123456789012345678901234, 38, 4);
    assert_eq!(row["string_v"], Value::String("hello".to_string()));
    assert_eq!(row["blob_v"], Value::Bytes(b"hi".to_vec()));
    assert_eq!(
        row["uuid_v"],
        Value::String("00112233-4455-6677-8899-aabbccddeeff".to_string())
    );
    assert_eq!(row["date_v"], Value::Date(DateValue { days: 18263 }));
    assert_eq!(
        row["time_v"],
        Value::Time(TimeValue {
            unit: TimeUnit::Micros,
            value: 1_234_567,
        })
    );
    assert_eq!(
        row["time_ns_v"],
        Value::Time(TimeValue {
            unit: TimeUnit::Nanos,
            value: 1_234_567_890,
        })
    );
    assert_eq!(
        row["ts_v"],
        Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Micros,
            value: 1_234_567,
            timezone_utc: false,
        })
    );
    assert_eq!(
        row["ts_s_v"],
        Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Seconds,
            value: 1,
            timezone_utc: false,
        })
    );
    assert_eq!(
        row["ts_ms_v"],
        Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Millis,
            value: 1_234,
            timezone_utc: false,
        })
    );
    assert_eq!(
        row["ts_ns_v"],
        Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Nanos,
            value: 1_234_567_890,
            timezone_utc: false,
        })
    );
    assert_eq!(
        row["ts_tz_v"],
        Value::Timestamp(TimestampValue {
            unit: TimestampUnit::Micros,
            value: 1_234_567,
            timezone_utc: true,
        })
    );
    assert_eq!(
        row["interval_v"],
        Value::Interval(IntervalValue {
            months: 1,
            days: 2,
            micros: 3,
        })
    );
    assert_eq!(row["enum_v"], Value::String("ok".to_string()));

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_round_trips_nested_types() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    let result = client
        .query(
            "
            SELECT
              [1, NULL, 3]::INTEGER[] AS ints,
              [[1, 2], [3, 4]]::INTEGER[][] AS nested_ints,
              {'x': 1::INTEGER, 'y': 'one'::VARCHAR} AS point,
              {'label': 'bag'::VARCHAR, 'items': [10, 20]::INTEGER[]} AS nested_struct,
              map(['a', 'b'], [1, 2]) AS map_v,
              array_value(7, 8, 9)::INTEGER[3] AS fixed_v
            ",
            None,
        )
        .await?;
    let (columns, rows) = result.into_rows();
    let types: Vec<LogicalTypeId> = columns
        .iter()
        .map(|column| column.logical_type.id)
        .collect();
    let rows: Vec<Row> = rows.try_collect().await?;

    assert_eq!(
        types,
        vec![
            LogicalTypeId::List,
            LogicalTypeId::List,
            LogicalTypeId::Struct,
            LogicalTypeId::Struct,
            LogicalTypeId::Map,
            LogicalTypeId::Array,
        ]
    );

    let row = &rows[0];
    assert_eq!(
        row["ints"],
        Value::List(vec![Value::Int(1), Value::Null, Value::Int(3)])
    );
    assert_eq!(
        row["nested_ints"],
        Value::List(vec![
            Value::List(vec![Value::Int(1), Value::Int(2)]),
            Value::List(vec![Value::Int(3), Value::Int(4)]),
        ])
    );
    assert_eq!(
        row["point"],
        struct_value(vec![
            ("x", Value::Int(1)),
            ("y", Value::String("one".to_string())),
        ])
    );
    assert_eq!(
        row["nested_struct"],
        struct_value(vec![
            ("label", Value::String("bag".to_string())),
            ("items", Value::List(vec![Value::Int(10), Value::Int(20)]),),
        ])
    );
    assert_eq!(
        row["map_v"],
        Value::List(vec![
            struct_value(vec![
                ("key", Value::String("a".to_string())),
                ("value", Value::Int(1)),
            ]),
            struct_value(vec![
                ("key", Value::String("b".to_string())),
                ("value", Value::Int(2)),
            ]),
        ])
    );
    assert_eq!(
        row["fixed_v"],
        Value::List(vec![Value::Int(7), Value::Int(8), Value::Int(9)])
    );

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_fetches_large_results_and_sequence_vectors() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    let result = client
        .query("SELECT i FROM range(5000) t(i) ORDER BY i", None)
        .await?;
    let (columns, rows) = result.into_rows();
    let first_name = columns[0].name.as_str();
    let rows: Vec<Row> = rows.try_collect().await?;
    let values: Vec<Value> = rows
        .iter()
        .map(|row| row.get(first_name).cloned().unwrap_or(Value::Null))
        .collect();

    assert_eq!(values.len(), 5000);
    assert_eq!(values.first(), Some(&Value::Int(0)));
    assert_eq!(values.get(1234), Some(&Value::Int(1234)));
    assert_eq!(values.last(), Some(&Value::Int(4999)));

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_supports_parameterized_queries() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    let result = client
        .query_with_params(
            "SELECT ?::INTEGER AS id, ?::VARCHAR AS label, ?::INTEGER[] AS values",
            Some(&SqlParameters::Positional(vec![
                SqlParameter::from(7),
                SqlParameter::from("seven"),
                SqlParameter::List(vec![
                    SqlParameter::from(1),
                    SqlParameter::from(2),
                    SqlParameter::from(3),
                ]),
            ])),
        )
        .await?;
    let (_, rows) = result.into_rows();
    let rows: Vec<Row> = rows.try_collect().await?;
    assert_eq!(
        rows,
        vec![row(vec![
            ("id", Value::Int(7)),
            ("label", Value::String("seven".to_string())),
            (
                "values",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
        ])]
    );

    let mut named = IndexMap::new();
    named.insert("id".to_string(), SqlParameter::from(8));
    named.insert("label".to_string(), SqlParameter::from("eight"));
    let result = client
        .query_with_params(
            "SELECT :id::INTEGER AS id, :label::VARCHAR AS label",
            Some(&SqlParameters::Named(named)),
        )
        .await?;
    let (_, rows) = result.into_rows();
    let rows: Vec<Row> = rows.try_collect().await?;
    assert_eq!(
        rows,
        vec![row(vec![
            ("id", Value::Int(8)),
            ("label", Value::String("eight".to_string())),
        ])]
    );

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_appends_scalar_and_nested_rows() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };
    let table = unique_name("quack_rust_append");
    // The stream is lazy: DDL only executes once the stream is polled.
    let (_, rows) = client
        .query(
            &format!(
                "
            CREATE TEMP TABLE {table} (
              id INTEGER,
              label VARCHAR,
              amount DECIMAL(10, 2),
              items INTEGER[],
              point STRUCT(x INTEGER, y VARCHAR),
              fixed INTEGER[3]
            )
            "
            ),
            None,
        )
        .await?
        .into_rows();
    let _: Vec<Row> = rows.try_collect().await?;

    client
        .append_rows(
            table.clone(),
            None,
            &[
                row(vec![
                    ("id", Value::Int(1)),
                    ("label", Value::String("one".to_string())),
                    ("amount", decimal_value("12.34", 10, 2)?),
                    ("items", Value::List(vec![Value::Int(1), Value::Int(2)])),
                    (
                        "point",
                        struct_value(vec![
                            ("x", Value::Int(10)),
                            ("y", Value::String("ten".to_string())),
                        ]),
                    ),
                    (
                        "fixed",
                        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                    ),
                ]),
                row(vec![
                    ("id", Value::Int(2)),
                    ("label", Value::String("two".to_string())),
                    ("amount", Value::Null),
                    ("items", Value::Null),
                    ("point", Value::Null),
                    (
                        "fixed",
                        Value::List(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                    ),
                ]),
            ],
            Some(vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    logical_type: LogicalTypes::integer(),
                },
                ColumnDefinition {
                    name: "label".to_string(),
                    logical_type: LogicalTypes::varchar(),
                },
                ColumnDefinition {
                    name: "amount".to_string(),
                    logical_type: LogicalTypes::decimal(10, 2),
                },
                ColumnDefinition {
                    name: "items".to_string(),
                    logical_type: LogicalTypes::list(LogicalTypes::integer()),
                },
                ColumnDefinition {
                    name: "point".to_string(),
                    logical_type: LogicalTypes::r#struct(vec![
                        ChildType {
                            name: "x".to_string(),
                            logical_type: LogicalTypes::integer(),
                        },
                        ChildType {
                            name: "y".to_string(),
                            logical_type: LogicalTypes::varchar(),
                        },
                    ]),
                },
                ColumnDefinition {
                    name: "fixed".to_string(),
                    logical_type: LogicalTypes::array(LogicalTypes::integer(), 3),
                },
            ]),
            Some(1),
        )
        .await?;

    let result = client
        .query(
            &format!("SELECT id, label, amount, items, point, fixed FROM {table} ORDER BY id"),
            None,
        )
        .await?;
    let (_, rows) = result.into_rows();
    let rows: Vec<Row> = rows.try_collect().await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], Value::Int(1));
    assert_eq!(rows[0]["label"], Value::String("one".to_string()));
    assert_decimal(&rows[0]["amount"], 1234, 10, 2);
    assert_eq!(
        rows[0]["items"],
        Value::List(vec![Value::Int(1), Value::Int(2)])
    );
    assert_eq!(
        rows[0]["point"],
        struct_value(vec![
            ("x", Value::Int(10)),
            ("y", Value::String("ten".to_string())),
        ])
    );
    assert_eq!(
        rows[0]["fixed"],
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    );
    assert_eq!(rows[1]["amount"], Value::Null);
    assert_eq!(rows[1]["items"], Value::Null);
    assert_eq!(rows[1]["point"], Value::Null);

    client.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn live_quack_surfaces_server_errors() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    // PREPARE runs during query(), so the server error surfaces at the await.
    let error = match client
        .query("SELECT * FROM definitely_missing_quack_rust_table", None)
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("query should fail"),
    };
    assert!(matches!(error, QuackError::Server(_)));

    client.disconnect().await?;
    Ok(())
}
