//! Rust client-side SDK for DuckDB's experimental Quack remote protocol.
//!
//! The crate implements the HTTP client transport plus the DuckDB
//! `BinarySerializer`-compatible message, logical type, and `DataChunk` codecs
//! needed by a Quack client.

mod binary;
mod builders;
mod client;
mod constants;
mod errors;
mod json;
mod logical_types;
mod messages;
mod sql;
mod values;
mod vector;

pub use builders::{ColumnDefinition, ColumnInput, column, data_chunk, data_chunk_from_rows};
pub use client::{
    QuackClient, QuackClientOptions, QuackConnectionInfo, QuackResultStream, QueryMetadata,
};
pub use errors::{QuackError, Result};
pub use futures_core::Stream;
pub use json::{
    BigIntJsonMode, BytesJsonMode, JsonOptions, TaggedJsonMode, to_json_row, to_json_rows,
    to_json_value,
};
pub use logical_types::{
    ChildType, CoordinateReferenceSystem, ExtraTypeInfo, LogicalType, LogicalTypeId, LogicalTypes,
};
pub use sql::{SqlParameter, SqlParameters, format_sql, sql_literal};
pub use values::{
    date_from_iso_date, date_value, decimal_value, interval_value, time_tz_value, time_value,
    timestamp_value,
};
pub use vector::{
    DataChunk, DateValue, DecimalValue, IntervalValue, Row, TimeTzValue, TimeUnit, TimeValue,
    TimestampUnit, TimestampValue, Value, decimal_to_string, rows_from_chunk,
};

#[cfg(test)]
mod protocol_tests;
