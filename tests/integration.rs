use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::TryStreamExt;
use indexmap::IndexMap;
use quack_protocol::*;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn live_options() -> QuackClientOptions {
    QuackClientOptions {
        auth_token: std::env::var("QUACK_AUTH_TOKEN").ok(),
        ..Default::default()
    }
}

async fn live_client() -> Result<Option<QuackClient>> {
    let uri = match std::env::var("QUACK_SERVER_URI") {
        Ok(uri) => uri,
        Err(_) => return Ok(None),
    };
    let client = QuackClient::connect(&uri, live_options()).await?;
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
async fn live_quack_reports_negotiated_version() -> Result<()> {
    let Some(client) = live_client().await? else {
        return Ok(());
    };

    let version = client
        .info
        .as_ref()
        .and_then(|info| info.quack_version)
        .expect("connected server should report its Quack protocol version");
    assert!(matches!(version, 1 | 3));
    if let Ok(expected) = std::env::var("QUACK_EXPECTED_VERSION") {
        assert_eq!(version.to_string(), expected);
    }

    client.disconnect().await?;
    Ok(())
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

// The connection model: one `QuackClient` is one server-side session, a
// `QuackPool` is several, and a session that nobody holds any more is closed.
//
// These tests need a server whose connection count belongs to them alone, so
// each starts its own Quack server inside the server under test - `quack_serve`
// is just SQL - and talks to that. `quack_server_list()` then reports exactly
// the connections the test made. Ports are per-test to keep them independent.
mod connection_model {
    use std::time::Duration;

    use futures_util::{StreamExt, TryStreamExt};
    use quack_protocol::{
        QuackClient, QuackPool, QuackPoolOptions, Result, SqlParameter, Value, sql_literal,
    };
    use tokio::time::{sleep, timeout};

    use super::{live_options, unique_name};

    // A server started by the tests is addressed by port, so each test picks
    // its own out of this block.
    const CONNECTIONS_ON_DROP_PORT: u16 = 19_601;
    const CONCURRENT_STREAMS_PORT: u16 = 19_602;
    const IDLE_REUSE_PORT: u16 = 19_603;
    const EXHAUSTED_POOL_PORT: u16 = 19_604;
    const RESTART_PORT: u16 = 19_605;
    const POOL_CLOSE_PORT: u16 = 19_606;
    const POOL_DROP_PORT: u16 = 19_607;
    const ABANDONED_STREAM_PORT: u16 = 19_608;

    // Control connection to the configured server, used to start, stop, and
    // inspect the per-test server. `None` when no live server is configured.
    async fn control() -> Result<Option<QuackClient>> {
        super::live_client().await
    }

    fn nested_uri(port: u16) -> String {
        format!("quack:127.0.0.1:{port}")
    }

    fn quoted(value: &str) -> Result<String> {
        sql_literal(&SqlParameter::from(value))
    }

    async fn start_server(control: &QuackClient, port: u16) -> Result<String> {
        let uri = nested_uri(port);
        // A previous run that panicked may have left this port serving.
        let _ = stop_server(control, port).await;
        let token = match std::env::var("QUACK_AUTH_TOKEN") {
            Ok(token) => format!(", token = {}", quoted(&token)?),
            Err(_) => String::new(),
        };
        control
            .execute(&format!("CALL quack_serve({}{token})", quoted(&uri)?), None)
            .await?;
        Ok(uri)
    }

    async fn stop_server(control: &QuackClient, port: u16) -> Result<()> {
        control
            .execute(
                &format!("CALL quack_stop({})", quoted(&nested_uri(port))?),
                None,
            )
            .await
    }

    async fn active_connections(control: &QuackClient, port: u16) -> Result<u64> {
        let values = control
            .values(&format!(
                "SELECT active_connections FROM quack_server_list() WHERE port = {port}"
            ))
            .await?;
        match values.first() {
            Some(Value::UInt(count)) => Ok(*count),
            other => panic!("unexpected active_connections {other:?}"),
        }
    }

    // Sessions are closed from a background task, so the count settles shortly
    // after the drop rather than at it.
    async fn await_connections(control: &QuackClient, port: u16, expected: u64) -> Result<()> {
        let mut last = active_connections(control, port).await?;
        for _ in 0..100 {
            if last == expected {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
            last = active_connections(control, port).await?;
        }
        panic!("expected {expected} connections on port {port}, still {last} after 5s");
    }

    #[tokio::test]
    async fn live_quack_closes_the_session_when_the_client_is_dropped() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = CONNECTIONS_ON_DROP_PORT;
        let uri = start_server(&control, port).await?;

        let client = QuackClient::connect(&uri, live_options()).await?;
        assert_eq!(
            client.values("SELECT 1::INTEGER").await?,
            vec![Value::Int(1)]
        );
        assert_eq!(active_connections(&control, port).await?, 1);

        // No disconnect call: dropping the last handle has to close the
        // session, or the server keeps its cursor for a connection id nobody
        // can use again.
        drop(client);
        await_connections(&control, port, 0).await?;

        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_streams_from_several_sessions_at_once() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = CONCURRENT_STREAMS_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 3 },
        )
        .await?;

        // Three result streams open at the same time, each holding its own
        // session. On a single client the second query would block behind the
        // first stream, so this both times out and hangs without a pool.
        let opened = timeout(Duration::from_secs(30), async {
            let mut streams = Vec::new();
            for _ in 0..3 {
                let (_, chunks) = pool
                    .query("SELECT i FROM range(100000) t(i)", None)
                    .await?
                    .into_chunks();
                streams.push(chunks);
            }
            Ok::<_, quack_protocol::QuackError>(streams)
        })
        .await
        .expect("three concurrent streams should open")?;
        assert_eq!(active_connections(&control, port).await?, 3);

        for chunks in opened {
            let rows: usize = chunks
                .try_fold(
                    0usize,
                    |rows, chunk| async move { Ok(rows + chunk.row_count) },
                )
                .await?;
            assert_eq!(rows, 100_000);
        }

        pool.close().await?;
        await_connections(&control, port, 0).await?;
        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_reuses_idle_connections() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = IDLE_REUSE_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 4 },
        )
        .await?;

        // Sequential queries hand the same session back and forth instead of
        // opening one per query.
        for i in 0..20 {
            assert_eq!(pool.values("SELECT 1::INTEGER").await?, vec![Value::Int(1)]);
            assert_eq!(
                active_connections(&control, port).await?,
                1,
                "query {i} opened a second connection"
            );
        }

        pool.close().await?;
        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_lease_keeps_one_session() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = EXHAUSTED_POOL_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 1 },
        )
        .await?;

        let table = unique_name("quack_rust_pool_temp");
        let lease = pool.acquire().await?;
        lease
            .execute(
                &format!("CREATE TEMP TABLE {table} AS SELECT 7::INTEGER AS answer"),
                None,
            )
            .await?;
        // Session state follows the lease, which is the reason to take one.
        assert_eq!(
            lease.values(&format!("SELECT answer FROM {table}")).await?,
            vec![Value::Int(7)]
        );

        // The pool is exhausted while the lease is out.
        assert!(
            timeout(Duration::from_millis(250), pool.acquire())
                .await
                .is_err(),
            "acquire should wait for the only connection"
        );

        drop(lease);
        let next = timeout(Duration::from_secs(5), pool.acquire())
            .await
            .expect("a returned connection should unblock acquire")?;
        // Same session, handed back: the temp table is still there.
        assert_eq!(
            next.values(&format!("SELECT answer FROM {table}")).await?,
            vec![Value::Int(7)]
        );
        drop(next);

        pool.close().await?;
        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_recycles_a_connection_from_an_abandoned_stream() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = ABANDONED_STREAM_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 1 },
        )
        .await?;

        // A `LIMIT`-shaped read: take one chunk and walk away, leaving the
        // server holding the rest of the cursor.
        let (_, mut chunks) = pool
            .query("SELECT i FROM range(100000) t(i)", None)
            .await?
            .into_chunks();
        assert!(chunks.next().await.expect("a first chunk")?.row_count > 0);
        drop(chunks);

        // The pool's only connection comes back, and the next PREPARE on it
        // clears the abandoned cursor rather than tripping over it.
        assert_eq!(pool.values("SELECT 5::INTEGER").await?, vec![Value::Int(5)]);
        assert_eq!(active_connections(&control, port).await?, 1);

        pool.close().await?;
        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_replaces_connections_the_server_forgot() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = RESTART_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 2 },
        )
        .await?;
        let client = QuackClient::connect(&uri, live_options()).await?;
        assert_eq!(pool.values("SELECT 1::INTEGER").await?, vec![Value::Int(1)]);

        // Restart the server behind their backs: every session opened against
        // the old one is gone.
        stop_server(&control, port).await?;
        start_server(&control, port).await?;

        // The pool retires the stale connection and retries on a fresh one.
        // PREPARE is rejected before any SQL runs, so the retry cannot repeat
        // a statement.
        assert_eq!(pool.values("SELECT 2::INTEGER").await?, vec![Value::Int(2)]);

        // A plain client does not reconnect: it is one session, and silently
        // opening another would drop the session state that came with it.
        let error = client
            .values("SELECT 3::INTEGER")
            .await
            .expect_err("a stale session should surface");
        assert!(error.is_connection_lost(), "{error}");

        pool.close().await?;
        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_closes_every_session() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = POOL_CLOSE_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 2 },
        )
        .await?;

        let first = pool.acquire().await?;
        let second = pool.acquire().await?;
        assert_eq!(active_connections(&control, port).await?, 2);
        drop(first);
        drop(second);

        pool.close().await?;
        assert_eq!(active_connections(&control, port).await?, 0);

        let error = pool
            .values("SELECT 1::INTEGER")
            .await
            .expect_err("a closed pool should refuse queries");
        assert!(error.to_string().contains("closed"), "{error}");

        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_closes_its_sessions_when_dropped() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let port = POOL_DROP_PORT;
        let uri = start_server(&control, port).await?;
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 2 },
        )
        .await?;
        let first = pool.acquire().await?;
        let second = pool.acquire().await?;
        drop(first);
        drop(second);
        assert_eq!(active_connections(&control, port).await?, 2);

        // No close call: the pool's connections are closed as it goes away.
        drop(pool);
        await_connections(&control, port, 0).await?;

        stop_server(&control, port).await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_reports_server_info_and_size() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let uri = std::env::var("QUACK_SERVER_URI").expect("configured above");
        let pool = QuackPool::connect(
            &uri,
            live_options(),
            QuackPoolOptions { max_connections: 2 },
        )
        .await?;
        assert_eq!(pool.max_connections(), 2);
        assert!(pool.info().is_some());
        assert!(
            pool.info()
                .and_then(|info| info.quack_version)
                .is_some_and(|version| version >= 1)
        );
        pool.close().await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_surfaces_sql_errors_without_retiring_connections() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let uri = std::env::var("QUACK_SERVER_URI").expect("configured above");
        let pool = QuackPool::connect(&uri, live_options(), QuackPoolOptions::default()).await?;

        let error = match pool
            .query("SELECT * FROM definitely_missing_quack_rust_table", None)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("query should fail"),
        };
        assert!(!error.is_connection_fatal(), "{error}");

        // A failed statement leaves the session usable.
        assert_eq!(pool.values("SELECT 8::INTEGER").await?, vec![Value::Int(8)]);
        pool.close().await?;
        control.disconnect().await
    }

    #[tokio::test]
    async fn live_quack_pool_appends_rows() -> Result<()> {
        let Some(control) = control().await? else {
            return Ok(());
        };
        let uri = std::env::var("QUACK_SERVER_URI").expect("configured above");
        let pool = QuackPool::connect(&uri, live_options(), QuackPoolOptions::default()).await?;

        // An append lands in a table the whole server shares, so any session
        // can read it back - unlike a temp table.
        let table = unique_name("quack_rust_pool_append");
        pool.execute(
            &format!("CREATE TABLE {table} (id INTEGER, label VARCHAR)"),
            None,
        )
        .await?;
        let rows = vec![
            super::row(vec![
                ("id", Value::Int(1)),
                ("label", Value::String("one".to_string())),
            ]),
            super::row(vec![
                ("id", Value::Int(2)),
                ("label", Value::String("two".to_string())),
            ]),
        ];
        pool.append_rows(table.clone(), None, &rows, None, None)
            .await?;

        assert_eq!(
            pool.values(&format!("SELECT id FROM {table} ORDER BY id"))
                .await?,
            vec![Value::Int(1), Value::Int(2)]
        );
        pool.execute(&format!("DROP TABLE {table}"), None).await?;
        pool.close().await?;
        control.disconnect().await
    }
}

#[cfg(feature = "arrow")]
mod arrow_output {
    use std::sync::Arc;

    use futures_util::TryStreamExt;
    use quack_protocol::arrow::arrow_array::cast::AsArray;
    use quack_protocol::arrow::arrow_array::types::Int32Type;
    use quack_protocol::arrow::arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch, StringArray, StructArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use quack_protocol::arrow::arrow_buffer::{IntervalMonthDayNano, i256};
    use quack_protocol::arrow::arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
    use quack_protocol::{QuackError, Result};

    use super::{live_client, unique_name};

    fn column_as<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("column {name} has an unexpected Arrow type"))
    }

    #[tokio::test]
    async fn live_quack_arrow_round_trips_scalar_types() -> Result<()> {
        let Some(client) = live_client().await? else {
            return Ok(());
        };
        let enum_name = unique_name("quack_rust_arrow_mood");
        let (_, batches) = client
            .query(
                &format!("CREATE TYPE {enum_name} AS ENUM ('sad', 'ok', 'happy')"),
                None,
            )
            .await?
            .into_record_batches()?;
        let _: Vec<RecordBatch> = batches.try_collect().await?;

        let (schema, batches) = client
            .query(
                &format!(
                    "
            SELECT
              -- The server binds an untyped NULL literal to INTEGER.
              NULL AS null_v,
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
              '5b1c9df8-4d0f-4c1e-9a2b-3c4d5e6f7a8b'::UUID AS uuid_v,
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
            .await?
            .into_record_batches()?;
        let batches: Vec<RecordBatch> = batches.try_collect().await?;

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Int32,
                DataType::Boolean,
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::UInt8,
                DataType::UInt16,
                DataType::UInt32,
                DataType::UInt64,
                DataType::Decimal256(39, 0),
                DataType::Decimal256(39, 0),
                DataType::Float32,
                DataType::Float64,
                DataType::Decimal128(4, 2),
                DataType::Decimal128(9, 2),
                DataType::Decimal128(18, 2),
                DataType::Decimal128(38, 4),
                DataType::Utf8,
                DataType::Binary,
                DataType::Utf8,
                DataType::Date32,
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Time64(TimeUnit::Nanosecond),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Utf8,
            ]
        );
        assert!(schema.fields().iter().all(|field| field.is_nullable()));

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.column_by_name("null_v").unwrap().null_count(), 1);
        assert!(column_as::<BooleanArray>(batch, "bool_v").value(0));
        assert_eq!(column_as::<Int8Array>(batch, "tiny_v").value(0), 127);
        assert_eq!(column_as::<Int16Array>(batch, "small_v").value(0), 32767);
        assert_eq!(column_as::<Int32Array>(batch, "int_v").value(0), 2147483647);
        assert_eq!(
            column_as::<Int64Array>(batch, "big_v").value(0),
            9007199254740993
        );
        assert_eq!(column_as::<UInt8Array>(batch, "utiny_v").value(0), 255);
        assert_eq!(column_as::<UInt16Array>(batch, "usmall_v").value(0), 65535);
        assert_eq!(
            column_as::<UInt32Array>(batch, "uint_v").value(0),
            4294967295
        );
        assert_eq!(column_as::<UInt64Array>(batch, "ubig_v").value(0), u64::MAX);
        assert_eq!(
            column_as::<Decimal256Array>(batch, "huge_v").value(0),
            i256::from_i128(123456789012345678901234567890)
        );
        assert_eq!(
            column_as::<Decimal256Array>(batch, "uhuge_v").value(0),
            i256::from_i128(123456789012345678901234567890)
        );
        assert_eq!(column_as::<Float32Array>(batch, "float_v").value(0), 1.5);
        assert_eq!(column_as::<Float64Array>(batch, "double_v").value(0), 2.25);
        assert_eq!(
            column_as::<Decimal128Array>(batch, "dec16_v").value(0),
            1234
        );
        assert_eq!(
            column_as::<Decimal128Array>(batch, "dec32_v").value(0),
            123456789
        );
        assert_eq!(
            column_as::<Decimal128Array>(batch, "dec64_v").value(0),
            123456789012345678
        );
        assert_eq!(
            column_as::<Decimal128Array>(batch, "dec128_v").value(0),
            1234567890123456789012345678901234
        );
        assert_eq!(
            column_as::<StringArray>(batch, "string_v").value(0),
            "hello"
        );
        assert_eq!(column_as::<BinaryArray>(batch, "blob_v").value(0), b"hi");
        assert_eq!(
            column_as::<StringArray>(batch, "uuid_v").value(0),
            "5b1c9df8-4d0f-4c1e-9a2b-3c4d5e6f7a8b"
        );
        assert_eq!(column_as::<Date32Array>(batch, "date_v").value(0), 18263);
        assert_eq!(
            column_as::<Time64MicrosecondArray>(batch, "time_v").value(0),
            1_234_567
        );
        assert_eq!(
            column_as::<Time64NanosecondArray>(batch, "time_ns_v").value(0),
            1_234_567_890
        );
        assert_eq!(
            column_as::<TimestampMicrosecondArray>(batch, "ts_v").value(0),
            1_234_567
        );
        assert_eq!(
            column_as::<TimestampSecondArray>(batch, "ts_s_v").value(0),
            1
        );
        assert_eq!(
            column_as::<TimestampMillisecondArray>(batch, "ts_ms_v").value(0),
            1_234
        );
        assert_eq!(
            column_as::<TimestampNanosecondArray>(batch, "ts_ns_v").value(0),
            1_234_567_890
        );
        assert_eq!(
            column_as::<TimestampMicrosecondArray>(batch, "ts_tz_v").value(0),
            1_234_567
        );
        assert_eq!(
            column_as::<IntervalMonthDayNanoArray>(batch, "interval_v").value(0),
            IntervalMonthDayNano::new(1, 2, 3_000)
        );
        assert_eq!(column_as::<StringArray>(batch, "enum_v").value(0), "ok");

        client.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_quack_arrow_round_trips_nested_types() -> Result<()> {
        let Some(client) = live_client().await? else {
            return Ok(());
        };

        let (schema, batches) = client
            .query(
                "
            SELECT
              [1, NULL, 3]::INTEGER[] AS ints,
              [[1, 2], [3, 4]]::INTEGER[][] AS nested_ints,
              {'x': 1::INTEGER, 'y': 'one'::VARCHAR} AS point,
              [{'x': 5::INTEGER, 'y': 'five'::VARCHAR}, NULL] AS points,
              map(['a', 'b'], [1, 2]) AS map_v,
              array_value(7, 8, 9)::INTEGER[3] AS fixed_v
            ",
                None,
            )
            .await?
            .into_record_batches()?;
        let batches: Vec<RecordBatch> = batches.try_collect().await?;
        let batch = &batches[0];

        let point_fields = vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Utf8, true),
        ];
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true
                ))),
                DataType::Struct(point_fields.clone().into()),
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(point_fields.clone().into()),
                    true
                ))),
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into()
                    ),
                    true
                ))),
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
            ]
        );

        let ints = column_as::<ListArray>(batch, "ints").value(0);
        assert_eq!(
            ints.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(1), None, Some(3)])
        );

        let nested = column_as::<ListArray>(batch, "nested_ints").value(0);
        let nested = nested.as_list::<i32>();
        assert_eq!(nested.len(), 2);
        assert_eq!(
            nested.value(1).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![3, 4])
        );

        let point = column_as::<StructArray>(batch, "point");
        assert_eq!(
            point.column(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1])
        );
        assert_eq!(
            point.column(1).as_string::<i32>(),
            &StringArray::from(vec!["one"])
        );

        let points = column_as::<ListArray>(batch, "points").value(0);
        let points = points.as_struct();
        assert_eq!(points.len(), 2);
        assert!(points.is_null(1));
        assert_eq!(
            points.column(1).as_string::<i32>(),
            &StringArray::from(vec![Some("five"), None])
        );

        let entries = column_as::<ListArray>(batch, "map_v").value(0);
        let entries = entries.as_struct();
        assert_eq!(
            entries.column(0).as_string::<i32>(),
            &StringArray::from(vec!["a", "b"])
        );
        assert_eq!(
            entries.column(1).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1, 2])
        );

        let fixed = column_as::<FixedSizeListArray>(batch, "fixed_v").value(0);
        assert_eq!(
            fixed.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![7, 8, 9])
        );

        client.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_quack_arrow_preserves_empty_result_schema() -> Result<()> {
        let Some(client) = live_client().await? else {
            return Ok(());
        };

        let (schema, batches) = client
            .query(
                "SELECT 1::INTEGER AS id, 'x'::VARCHAR AS label WHERE 1 = 0",
                None,
            )
            .await?
            .into_record_batches()?;
        let batches: Vec<RecordBatch> = batches.try_collect().await?;

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                ("id".to_string(), DataType::Int32),
                ("label".to_string(), DataType::Utf8),
            ]
        );
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

        client.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_quack_arrow_streams_multiple_batches_with_one_schema() -> Result<()> {
        let Some(client) = live_client().await? else {
            return Ok(());
        };

        let (schema, batches) = client
            .query("SELECT i FROM range(5000) t(i) ORDER BY i", None)
            .await?
            .into_record_batches()?;
        let batches: Vec<RecordBatch> = batches.try_collect().await?;

        assert!(batches.len() > 1, "expected a multi-chunk result");
        assert!(
            batches
                .iter()
                .all(|batch| Arc::ptr_eq(&batch.schema(), &schema))
        );
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                column_as::<Int64Array>(batch, schema.field(0).name())
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values.len(), 5000);
        assert_eq!(values.first(), Some(&0));
        assert_eq!(values.last(), Some(&4999));

        client.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_quack_arrow_passes_variant_through_as_a_struct() -> Result<()> {
        let Some(client) = live_client().await? else {
            return Ok(());
        };

        let (schema, batches) = client
            .query("SELECT {'x': 1, 'y': 'two'}::VARIANT AS variant_v", None)
            .await?
            .into_record_batches()?;
        let batches: Vec<RecordBatch> = batches.try_collect().await?;

        // VARIANT arrives as the struct DuckDB shreds it into, the same shape
        // the Value path already exposes.
        let DataType::Struct(fields) = schema.field(0).data_type() else {
            panic!("VARIANT should map to a struct, got {:?}", schema.field(0));
        };
        assert_eq!(
            fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
            ["keys", "children", "values", "data"]
        );

        let column = batches[0].column(0).as_struct();
        let keys = column.column_by_name("keys").unwrap().as_list::<i32>();
        assert_eq!(
            keys.value(0).as_string::<i32>(),
            &StringArray::from(vec!["x", "y"])
        );
        assert!(
            !column
                .column_by_name("data")
                .unwrap()
                .as_binary::<i32>()
                .value(0)
                .is_empty(),
            "the variant payload should carry its encoded data"
        );

        client.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_quack_arrow_rejects_unsupported_types() -> Result<()> {
        let Some(client) = live_client().await? else {
            return Ok(());
        };

        let error = client
            .query("SELECT '12:34:56+02'::TIMETZ AS time_tz_v", None)
            .await?
            .into_record_batches()
            .err()
            .expect("TIMETZ has no Arrow mapping");

        assert!(
            matches!(&error, QuackError::UnsupportedType(message) if message.contains("TimeTz")),
            "unexpected error: {error}"
        );

        client.disconnect().await?;
        Ok(())
    }
}
