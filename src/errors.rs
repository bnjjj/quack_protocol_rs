pub type Result<T> = std::result::Result<T, QuackError>;

// The current protocol has no typed error code. Exact comparison keeps this
// retirement heuristic narrower than arbitrary SQL error text containing one
// of these server messages. It must never be used to authorize replay.
const RETIRE_CONNECTION_MESSAGES: [&str; 2] = [
    "Invalid connection id",
    "Connection does not exist / already disconnected",
];

#[derive(Debug, thiserror::Error)]
pub enum QuackError {
    #[error("quack protocol error: {0}")]
    Protocol(String),
    #[error("quack server error: {0}")]
    Server(String),
    #[error("unsupported quack type: {0}")]
    UnsupportedType(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
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

    /// The connection that produced this error should not be reused.
    ///
    /// Transport and framing failures leave no way to tell what the server did
    /// with the request, so a pool retires the connection rather than hand it
    /// to the next caller. Errors raised without touching the wire - bad
    /// arguments, unsupported types - are not counted.
    ///
    /// This says nothing about whether the request ran. Current protocol
    /// versions carry only an error message string, and SQL can produce the
    /// same text as a missing-connection response, so no error is safe to
    /// retry automatically.
    pub fn is_connection_fatal(&self) -> bool {
        match self {
            Self::Http(_) | Self::Protocol(_) => true,
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
