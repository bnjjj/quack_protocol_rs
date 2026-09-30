//! Bounded metadata inspection for HTTP proxies. Payloads remain opaque.
//!
//! Headers share the client's codec. Incomplete input is distinct from invalid
//! input; callers must stop collecting at [`MAX_MESSAGE_HEADER_BYTES`]. Neither this
//! API nor its errors expose SQL, authentication fields, or upstream error text.

pub use crate::messages::MessageType as Operation;
use crate::{
    QuackError,
    binary::BinaryReader,
    messages::{self, QuackMessage},
};

pub const MAX_MESSAGE_HEADER_BYTES: usize = 4096;
pub const MAX_CONTROL_RESPONSE_BYTES: usize = 16 * 1024;
pub const MAX_CONNECTION_ID_BYTES: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageHeader {
    pub operation: Operation,
    pub connection_id: Option<String>,
    pub client_query_id: Option<u64>,
    /// Number of bytes occupied by the header; excludes the opaque body.
    pub encoded_len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InspectionError {
    #[error("incomplete quack message header")]
    Incomplete,
    #[error("invalid quack message header")]
    Invalid,
    #[error("quack metadata exceeds inspection limit")]
    TooLarge,
}

/// Inspects only the header, even when `prefix` also contains a large payload.
pub fn inspect_message_header(prefix: &[u8]) -> Result<MessageHeader, InspectionError> {
    let bounded = &prefix[..prefix.len().min(MAX_MESSAGE_HEADER_BYTES)];
    let mut reader = BinaryReader::new(bounded);
    let header = match messages::decode_header(&mut reader) {
        Ok(header) => header,
        Err(_) if reader.is_incomplete() => {
            return Err(if bounded.len() == MAX_MESSAGE_HEADER_BYTES {
                InspectionError::TooLarge
            } else {
                InspectionError::Incomplete
            });
        }
        Err(_) => return Err(InspectionError::Invalid),
    };
    if header
        .connection_id
        .as_ref()
        .is_some_and(|id| id.len() > MAX_CONNECTION_ID_BYTES)
    {
        return Err(InspectionError::TooLarge);
    }
    Ok(MessageHeader {
        operation: header.message_type,
        connection_id: header.connection_id,
        client_query_id: header.client_query_id,
        encoded_len: bounded.len() - reader.remaining(),
    })
}

/// Only bounded control responses are decoded. Never call on result payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlResponse {
    Connected { connection_id: String },
    Success,
    Error { fatal: bool },
}

pub fn inspect_control_response(bytes: &[u8]) -> Result<ControlResponse, InspectionError> {
    if bytes.len() > MAX_CONTROL_RESPONSE_BYTES {
        return Err(InspectionError::TooLarge);
    }
    let header = inspect_message_header(bytes)?;
    if !matches!(
        header.operation,
        Operation::ConnectionResponse | Operation::SuccessResponse | Operation::ErrorResponse
    ) {
        return Err(InspectionError::Invalid);
    }
    if header.operation == Operation::ErrorResponse {
        let mut reader = BinaryReader::new(&bytes[header.encoded_len..]);
        let (message, invalidated) = reader
            .read_object(messages::read_error_fields)
            .map_err(|_| InspectionError::Invalid)?;
        reader.assert_eof().map_err(|_| InspectionError::Invalid)?;
        // Legacy versions carry only text. This conservative fallback can cause
        // recycling, but must never authorize replay or release ownership.
        let fatal = invalidated
            || message.starts_with("FATAL Error:")
            || message.starts_with("INTERNAL Error:")
            || message.starts_with("Fatal Error:")
            || message.starts_with("Internal Error:")
            || QuackError::server(message).is_connection_fatal();
        return Ok(ControlResponse::Error { fatal });
    }
    // These bodies are compatible across v1 and v3; v3's added fields are optional.
    let message = messages::decode_message_for_version(bytes, crate::constants::QUACK_V3)
        .map_err(|_| InspectionError::Invalid)?;
    match message {
        QuackMessage::ConnectionResponse {
            header,
            quack_version,
            ..
        } => {
            if quack_version.is_some_and(|v| v != 1 && v != 3) {
                return Err(InspectionError::Invalid);
            }
            Ok(ControlResponse::Connected {
                connection_id: header.connection_id.ok_or(InspectionError::Invalid)?,
            })
        }
        QuackMessage::SuccessResponse { .. } => Ok(ControlResponse::Success),
        _ => Err(InspectionError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        binary::BinaryWriter,
        messages::{MessageHeader as WireMessageHeader, encode_header, encode_message},
    };

    fn header(operation: Operation, connection: Option<&str>) -> WireMessageHeader {
        WireMessageHeader {
            message_type: operation,
            connection_id: connection.map(str::to_owned),
            client_query_id: Some(42),
        }
    }

    #[test]
    fn every_request_header_accepts_all_chunk_boundaries_without_payload() {
        for operation in [
            Operation::ConnectionRequest,
            Operation::PrepareRequest,
            Operation::FetchRequest,
            Operation::SendDataRequest,
            Operation::DisconnectMessage,
            Operation::CancelRequest,
            Operation::Acknowledgement,
            Operation::HeartbeatRequest,
        ] {
            let mut writer = BinaryWriter::new();
            encode_header(&mut writer, &header(operation, Some("connection-example"))).unwrap();
            let mut bytes = writer.into_bytes();
            for end in 0..bytes.len() {
                assert_eq!(
                    inspect_message_header(&bytes[..end]),
                    Err(InspectionError::Incomplete)
                );
            }
            let expected = inspect_message_header(&bytes).unwrap();
            assert_eq!(expected.operation, operation);
            assert_eq!(expected.client_query_id, Some(42));
            assert_eq!(expected.encoded_len, bytes.len());
            bytes.resize(1024 * 1024, 0xfe);
            assert_eq!(inspect_message_header(&bytes).unwrap(), expected);
        }
    }

    #[test]
    fn malformed_and_oversized_headers_do_not_panic_or_echo_input() {
        assert_eq!(
            inspect_message_header(&[2, 0, 1]),
            Err(InspectionError::Invalid)
        );
        let mut writer = BinaryWriter::new();
        encode_header(
            &mut writer,
            &header(Operation::PrepareRequest, Some(&"x".repeat(257))),
        )
        .unwrap();
        assert_eq!(
            inspect_message_header(writer.as_slice()),
            Err(InspectionError::TooLarge)
        );
        for length in 0..MAX_MESSAGE_HEADER_BYTES + 20 {
            let bytes = vec![0xff; length];
            assert!(inspect_message_header(&bytes).is_err());
        }
        // An attacker-controlled usize::MAX string length must not overflow ensure().
        let mut writer = BinaryWriter::new();
        writer.write_field(1, |w| w.write_uleb(3u64)).unwrap();
        writer.write_field(2, |w| w.write_uleb(u64::MAX)).unwrap();
        assert!(inspect_message_header(writer.as_slice()).is_err());
    }

    #[test]
    fn complete_control_response_is_required_and_errors_are_sanitized() {
        let success = encode_message(&QuackMessage::SuccessResponse {
            header: header(Operation::SuccessResponse, Some("example")),
        })
        .unwrap();
        assert_eq!(
            inspect_control_response(&success).unwrap(),
            ControlResponse::Success
        );
        assert!(inspect_control_response(&success[..success.len() - 1]).is_err());
        let mut trailing = success.clone();
        trailing.push(0);
        assert!(inspect_control_response(&trailing).is_err());
        for (message, fatal) in [
            ("Catalog Error: private query text", false),
            ("FATAL Error: private details", true),
            ("Invalid connection id", true),
        ] {
            let bytes = encode_message(&QuackMessage::ErrorResponse {
                header: header(Operation::ErrorResponse, None),
                message: message.into(),
            })
            .unwrap();
            assert_eq!(
                inspect_control_response(&bytes).unwrap(),
                ControlResponse::Error { fatal }
            );
        }
    }

    #[test]
    fn structured_invalidation_recycles_and_malicious_extra_info_is_bounded() {
        for count in [0, u64::MAX] {
            let mut writer = BinaryWriter::new();
            encode_header(&mut writer, &header(Operation::ErrorResponse, None)).unwrap();
            writer
                .write_object(|w| {
                    w.write_field(1, |w| w.write_string("private details"))?;
                    w.write_field(2, |w| w.write_string("Catalog"))?;
                    w.write_field(3, |w| w.write_uleb(count))?;
                    w.write_field(4, |w| w.write_bool(true))
                })
                .unwrap();
            if count == 0 {
                assert_eq!(
                    inspect_control_response(writer.as_slice()).unwrap(),
                    ControlResponse::Error { fatal: true }
                );
            } else {
                assert!(inspect_control_response(writer.as_slice()).is_err());
            }
        }
    }
}
