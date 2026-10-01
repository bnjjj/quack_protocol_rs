//! Server-side codec, behind the `server` feature.
//!
//! A Quack server decodes the messages a client sends (CONNECTION, PREPARE,
//! FETCH, HEARTBEAT, CANCEL, ACKNOWLEDGEMENT, DISCONNECT) and encodes the
//! responses. Protocol v3 (DuckDB 2.0) is the version a server speaks.
//!
//! Result rows can be encoded two ways. [`QuackMessage::PrepareResponse`] and
//! [`QuackMessage::FetchResponse`] carry [`DataChunk`]s of decoded values. A
//! server that builds its chunks itself (for example straight from Arrow
//! arrays, with [`BinaryWriter`] and [`write_string_vector_data`]) passes the
//! encoded chunks to [`encode_prepare_response`] and [`encode_fetch_response`]
//! instead.

use crate::constants::QUACK_V3;
use crate::errors::{QuackError, Result};

pub use crate::binary::{
    BinaryReader, BinaryWriter, HugeIntParts, combine_signed_huge_int, split_signed_huge_int,
};
pub use crate::constants::OPTIONAL_INDEX_INVALID;
pub use crate::logical_types::{decode_logical_type, encode_logical_type};
pub use crate::messages::{
    MessageHeader, MessageType, QuackMessage, decode_header, decode_message_for_version,
    encode_header, encode_message_for_version,
};
pub use crate::vector::{
    DataChunk, StringLayout, decode_data_chunk, encode_data_chunk_with_layout, validity_mask_size,
    write_string_vector_data, write_validity_mask,
};

use crate::logical_types::LogicalType;

/// The protocol version this module encodes and decodes.
pub const QUACK_VERSION: u64 = QUACK_V3;

/// Decodes a request body that a client sent.
pub fn decode_request(bytes: &[u8]) -> Result<QuackMessage> {
    decode_message_for_version(bytes, QUACK_VERSION)
}

/// Encodes a response message.
pub fn encode_response(message: &QuackMessage) -> Result<Vec<u8>> {
    encode_message_for_version(message, QUACK_VERSION)
}

/// Encodes a PREPARE_RESPONSE whose inline result chunks are already encoded.
///
/// Every element of `chunks` is one `DataChunk` object, as
/// [`encode_data_chunk_with_layout`] writes it.
pub fn encode_prepare_response(
    header: &MessageHeader,
    result_types: &[LogicalType],
    result_names: &[String],
    needs_more_fetch: bool,
    chunks: &[impl AsRef<[u8]>],
    query_uuid: HugeIntParts,
) -> Result<Vec<u8>> {
    expect_type(header, MessageType::PrepareResponse)?;
    let mut writer = BinaryWriter::with_capacity(
        256 + chunks.iter().map(|c| c.as_ref().len() + 8).sum::<usize>(),
    );
    encode_header(&mut writer, header)?;
    writer.write_object(|object| {
        if !result_types.is_empty() {
            object.write_field(1, |object| {
                object.write_list(result_types, |object, logical_type, _| {
                    encode_logical_type(object, logical_type)
                })
            })?;
        }
        if !result_names.is_empty() {
            object.write_field(2, |object| {
                object.write_list(result_names, |object, name, _| object.write_string(name))
            })?;
        }
        if needs_more_fetch {
            object.write_field(3, |object| object.write_bool(true))?;
        }
        if !chunks.is_empty() {
            object.write_field(4, |object| {
                object.write_list(chunks, |object, chunk, _| {
                    // unique_ptr<DataChunkWrapper>: present, then an object with the chunk in field 300
                    object.write_bool(true)?;
                    object.write_object(|wrapper| {
                        wrapper.write_field(300, |wrapper| wrapper.write_bytes(chunk.as_ref()))
                    })
                })
            })?;
        }
        object.write_field(5, |object| object.write_huge_int_parts(query_uuid))
    })?;
    Ok(writer.into_bytes())
}

/// Encodes a FETCH_RESPONSE whose result chunks are already encoded.
///
/// The chunks follow the message body as a raw blob, one `DataChunk` object
/// after another. A response with no chunks ends the stream, and should carry
/// `total_batches`.
pub fn encode_fetch_response(
    header: &MessageHeader,
    chunks: &[impl AsRef<[u8]>],
    total_batches: Option<u64>,
    batch_index: Option<u64>,
) -> Result<Vec<u8>> {
    expect_type(header, MessageType::FetchResponse)?;
    let mut writer =
        BinaryWriter::with_capacity(64 + chunks.iter().map(|c| c.as_ref().len()).sum::<usize>());
    encode_header(&mut writer, header)?;
    writer.write_object(|object| {
        if !chunks.is_empty() {
            object.write_field(1, |object| object.write_uleb(chunks.len() as u64))?;
        }
        if let Some(total_batches) = total_batches {
            object.write_field(2, |object| object.write_uleb(total_batches))?;
        }
        if let Some(batch_index) = batch_index {
            object.write_field(3, |object| object.write_uleb(batch_index))?;
        }
        Ok(())
    })?;
    for chunk in chunks {
        writer.write_bytes(chunk.as_ref())?;
    }
    Ok(writer.into_bytes())
}

fn expect_type(header: &MessageHeader, expected: MessageType) -> Result<()> {
    if header.message_type != expected {
        return Err(QuackError::protocol(format!(
            "header type {:?} does not match {expected:?}",
            header.message_type
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_types::LogicalTypes;
    use crate::vector::{DecodedVector, Value, VectorType};

    fn uuid() -> HugeIntParts {
        HugeIntParts {
            upper: 7,
            lower: 42,
        }
    }

    fn chunk() -> DataChunk {
        let types = vec![LogicalTypes::integer(), LogicalTypes::varchar()];
        let columns = vec![
            DecodedVector {
                logical_type: LogicalTypes::integer(),
                vector_type: VectorType::Flat,
                values: vec![Value::Int(1), Value::Null],
            },
            DecodedVector {
                logical_type: LogicalTypes::varchar(),
                vector_type: VectorType::Flat,
                values: vec![Value::String("a".into()), Value::String("bc".into())],
            },
        ];
        DataChunk {
            row_count: 2,
            types,
            columns,
            column_names: None,
        }
    }

    fn encoded_chunk() -> Vec<u8> {
        let mut writer = BinaryWriter::new();
        encode_data_chunk_with_layout(&mut writer, &chunk(), StringLayout::LengthsAndBytes)
            .unwrap();
        writer.into_bytes()
    }

    #[test]
    fn raw_prepare_response_matches_the_message_encoder() {
        let header = MessageHeader::new(MessageType::PrepareResponse);
        let message = QuackMessage::PrepareResponse {
            header: header.clone(),
            result_types: chunk().types,
            result_names: vec!["i".into(), "s".into()],
            needs_more_fetch: true,
            results: vec![chunk()],
            result_uuid: uuid(),
        };
        let raw = encode_prepare_response(
            &header,
            &chunk().types,
            &["i".to_string(), "s".to_string()],
            true,
            &[encoded_chunk()],
            uuid(),
        )
        .unwrap();
        assert_eq!(raw, encode_response(&message).unwrap());
        assert_eq!(decode_request(&raw).unwrap(), message);
    }

    #[test]
    fn raw_fetch_response_matches_the_message_encoder() {
        let header = MessageHeader::new(MessageType::FetchResponse);
        let message = QuackMessage::FetchResponse {
            header: header.clone(),
            results: vec![chunk(), chunk()],
            total_batches: None,
            batch_index: Some(3),
        };
        let raw =
            encode_fetch_response(&header, &[encoded_chunk(), encoded_chunk()], None, Some(3))
                .unwrap();
        assert_eq!(raw, encode_response(&message).unwrap());
        assert_eq!(decode_request(&raw).unwrap(), message);
    }

    #[test]
    fn end_of_stream_fetch_response_carries_the_total() {
        let header = MessageHeader::new(MessageType::FetchResponse);
        let raw = encode_fetch_response(&header, &[] as &[Vec<u8>], Some(5), None).unwrap();
        match decode_request(&raw).unwrap() {
            QuackMessage::FetchResponse {
                results,
                total_batches,
                batch_index,
                ..
            } => {
                assert!(results.is_empty());
                assert_eq!(total_batches, Some(5));
                assert_eq!(batch_index, None);
            }
            other => panic!("expected FETCH_RESPONSE, got {other:?}"),
        }
    }

    #[test]
    fn cancel_and_acknowledgement_round_trip() {
        for message in [
            QuackMessage::CancelRequest {
                header: MessageHeader::new(MessageType::CancelRequest).with_connection("c"),
                query_uuid: uuid(),
            },
            QuackMessage::CancelRequest {
                header: MessageHeader::new(MessageType::CancelRequest).with_connection("c"),
                query_uuid: HugeIntParts { upper: 0, lower: 0 },
            },
            QuackMessage::Acknowledgement {
                header: MessageHeader::new(MessageType::Acknowledgement).with_connection("c"),
                query_uuid: uuid(),
            },
        ] {
            let bytes = encode_response(&message).unwrap();
            assert_eq!(decode_request(&bytes).unwrap(), message);
        }
    }

    #[test]
    fn mismatched_header_type_is_rejected() {
        let header = MessageHeader::new(MessageType::SuccessResponse);
        assert!(encode_fetch_response(&header, &[] as &[Vec<u8>], None, None).is_err());
    }
}
