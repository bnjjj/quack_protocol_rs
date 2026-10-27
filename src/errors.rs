pub type Result<T> = std::result::Result<T, QuackError>;

// The current protocol has no typed error code. Exact comparison keeps this
// retirement heuristic narrower than arbitrary SQL error text containing one
// of these server messages. It must never be used to authorize replay.
const RETIRE_CONNECTION_MESSAGES: [&str; 2] = [
    "Invalid connection id",
    "Connection does not exist / already disconnected",
];

/// Largest non-success reply body kept in [`QuackError::HttpStatus`].
pub(crate) const MAX_HTTP_STATUS_BODY_BYTES: usize = 16 * 1024;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QuackError {
    #[error("quack protocol error: {0}")]
    Protocol(String),
    #[error("quack server error: {0}")]
    Server(String),
    #[error("unsupported quack type: {0}")]
    UnsupportedType(String),
    /// The HTTP request got a non-success status, from the Quack server or
    /// from something in front of it, such as a proxy. Either can put its own
    /// errors in `body`; the crate does not interpret them, and callers that
    /// know the format parse them. The body is raw and untrusted, never
    /// printed, and empty when it was larger than 16 KiB.
    #[non_exhaustive]
    #[error("quack HTTP request failed with status {status}")]
    HttpStatus { status: u16, body: Vec<u8> },
    /// The HTTP exchange itself failed: no complete reply was received.
    #[error("quack transport error ({kind}): {source}")]
    Transport {
        kind: TransportErrorKind,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// What part of an HTTP exchange failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// No connection to the server was made, so the request was not sent.
    /// A connection attempt that timed out counts here, not as `Timeout`.
    Connect,
    /// The request was sent, but no reply arrived in time.
    Timeout,
    Other,
}

impl std::fmt::Display for TransportErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Connect => "connect",
            Self::Timeout => "timeout",
            Self::Other => "other",
        })
    }
}

impl QuackError {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub(crate) fn server(message: impl Into<String>) -> Self {
        Self::Server(message.into())
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::UnsupportedType(message.into())
    }

    // Not a `From` impl: that would make reqwest part of the public API.
    pub(crate) fn transport(error: reqwest::Error) -> Self {
        let kind = if error.is_connect() {
            TransportErrorKind::Connect
        } else if error.is_timeout() {
            TransportErrorKind::Timeout
        } else {
            TransportErrorKind::Other
        };
        Self::Transport {
            kind,
            source: Box::new(error),
        }
    }

    /// The status and raw body of a non-success HTTP reply.
    pub fn http_status(&self) -> Option<(u16, &[u8])> {
        match self {
            Self::HttpStatus { status, body } => Some((*status, body)),
            _ => None,
        }
    }

    /// The connection that produced this error should not be reused.
    ///
    /// Transport and framing failures leave no way to tell what the server did
    /// with the request, so a pool retires the connection rather than hand it
    /// to the next caller. Non-success HTTP replies carry codes this crate
    /// does not interpret, so they retire it too: a harmless
    /// rejection costs one reconnect, while keeping a lost session would hand
    /// it out again and again. Errors raised without touching the wire - bad
    /// arguments, unsupported types - are not counted.
    ///
    /// This says nothing about whether the request ran. Current protocol
    /// versions carry only an error message string, and SQL can produce the
    /// same text as a missing-connection response, so no error is safe to
    /// retry automatically.
    pub(crate) fn is_connection_fatal(&self) -> bool {
        match self {
            Self::HttpStatus { .. } | Self::Transport { .. } | Self::Protocol(_) => true,
            Self::Server(message) => RETIRE_CONNECTION_MESSAGES
                .iter()
                .any(|known| message == known),
            Self::UnsupportedType(_) | Self::Url(_) | Self::Utf8(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_connection_error_text_retires() {
        for message in RETIRE_CONNECTION_MESSAGES {
            let err = QuackError::server(message);
            assert!(err.is_connection_fatal(), "{message}");
        }
    }

    #[test]
    fn sql_error_text_containing_connection_message_is_not_fatal() {
        for message in [
            "Invalid Input Error: Invalid connection id",
            "statement failed: Invalid connection id after committing",
            "Connection does not exist / already disconnected (from SQL)",
        ] {
            let err = QuackError::server(message);
            assert!(!err.is_connection_fatal(), "{message}");
        }
    }

    #[test]
    fn sql_errors_leave_the_connection_usable() {
        let err = QuackError::server("Table with name t does not exist!");
        assert!(!err.is_connection_fatal());
    }

    #[test]
    fn transport_errors_are_fatal() {
        let err = QuackError::protocol("expected PREPARE_RESPONSE, got FetchResponse");
        assert!(err.is_connection_fatal());
    }
}
