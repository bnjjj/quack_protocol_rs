//! Arrow output support for query results.
//!
//! The bridge is read-only: it maps an already decoded [`DataChunk`] onto
//! [`arrow_array::RecordBatch`] using the column
//! [`LogicalType`](crate::LogicalType)s, so the wire codecs are untouched. It
//! is gated behind the `arrow` feature.
//!
//! [`QuackResultStream::into_record_batches`] is the entry point; the module
//! otherwise only re-exports the `arrow-rs` crates the bridge builds against,
//! so a caller can match their versions.

mod columns;
mod schema;
#[cfg(test)]
mod tests;
mod values;

use std::iter::zip;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;

use crate::builders::ColumnDefinition;
use crate::client::QuackResultStream;
use crate::errors::{QuackError, Result};
use crate::logical_types::LogicalTypeId;
use crate::vector::{DataChunk, Value};

use self::columns::build_array;
use self::schema::field;

pub use arrow_array;
pub use arrow_buffer;
pub use arrow_schema;

const HUGE_INT_PRECISION: u8 = 39;
const UTC_TIMEZONE: &str = "UTC";

/// Builds the Arrow schema of a query result from its column definitions.
///
/// The definitions come from prepare time, so the schema is available even for
/// results that carry no chunks at all.
fn schema(columns: &[ColumnDefinition]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| field(&column.name, &column.logical_type))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Converts a decoded chunk into a [`RecordBatch`] with the given schema.
///
/// Returns an error if the chunk's column types do not match the schema.
fn to_record_batch(chunk: &DataChunk, schema: &SchemaRef) -> Result<RecordBatch> {
    if chunk.types.len() != schema.fields().len() {
        return Err(QuackError::protocol(format!(
            "DataChunk has {} columns but the Arrow schema declares {}",
            chunk.types.len(),
            schema.fields().len()
        )));
    }
    let mut arrays = Vec::with_capacity(chunk.types.len());
    for (index, (logical_type, field)) in zip(&chunk.types, schema.fields()).enumerate() {
        let values = chunk.column_values(index).ok_or_else(|| {
            QuackError::protocol(format!("DataChunk is missing column vector {index}"))
        })?;
        if values.len() != chunk.row_count {
            return Err(QuackError::protocol(format!(
                "column {index} has {} values, expected {}",
                values.len(),
                chunk.row_count
            )));
        }
        arrays.push(build_array(
            logical_type,
            field.data_type(),
            &values.iter().collect::<Vec<_>>(),
        )?);
    }
    RecordBatch::try_new_with_options(
        schema.clone(),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(chunk.row_count)),
    )
    .map_err(arrow_error)
}

impl QuackResultStream {
    /// Consumes the stream as Arrow record batches sharing a single schema.
    ///
    /// Like `into_chunks()`, the returned stream holds the client's connection
    /// until it is drained or dropped, so another query on the same client
    /// waits for it to finish.
    pub fn into_record_batches(
        self,
    ) -> Result<(SchemaRef, BoxStream<'static, Result<RecordBatch>>)> {
        let (columns, chunks) = self.into_chunks();
        let schema = schema(&columns)?;
        let batch_schema = schema.clone();
        let batches = chunks
            .map(move |chunk| chunk.and_then(|chunk| to_record_batch(&chunk, &batch_schema)))
            .boxed();
        Ok((schema, batches))
    }
}

fn unsupported(id: LogicalTypeId) -> QuackError {
    QuackError::unsupported(format!("logical type {id:?} has no Arrow mapping"))
}

fn mismatch(id: LogicalTypeId, value: &Value) -> QuackError {
    QuackError::protocol(format!(
        "decoded value {value:?} does not match logical type {id:?}"
    ))
}

fn out_of_range(id: LogicalTypeId, value: impl std::fmt::Display) -> QuackError {
    QuackError::protocol(format!(
        "decoded value {value} is out of range for logical type {id:?}"
    ))
}

fn schema_shape(expected: &str, actual: &DataType) -> QuackError {
    QuackError::protocol(format!(
        "Arrow schema declares {actual} where {expected} is required"
    ))
}

fn arrow_error(error: ArrowError) -> QuackError {
    QuackError::protocol(format!("arrow conversion failed: {error}"))
}
