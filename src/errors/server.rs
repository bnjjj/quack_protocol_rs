//! Structured server failures shared by the client and bounded wire inspector.

use std::{collections::BTreeMap, fmt};

use super::{ErrorSubtype, ExceptionType};

/// A server error, including metadata supplied by newer protocol implementations.
///
/// The client and inspector preserve unknown exception types and extra-info keys.
/// These values are untrusted and may contain SQL, credentials, or server paths.
/// `Debug` redacts them; `Display` retains the message for compatibility. Consumers
/// must apply their own disclosure policy before logging or forwarding the data.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ServerError {
    /// Original text; never safe to log implicitly.
    pub(crate) message: String,
    /// Optional native exception name, including unknown names.
    pub(crate) exception_type: Option<String>,
    /// Native string metadata, preserved without interpretation.
    pub(crate) extra_info: BTreeMap<String, String>,
    /// Explicit server invalidation signal, independent of inferred fatality.
    pub(crate) must_invalidate: bool,
}

impl ServerError {
    /// Creates a legacy error with a message and no structured metadata.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Self::default()
        }
    }

    /// Adds the server's exception type, preserving unknown values.
    pub fn with_exception_type(mut self, exception_type: impl Into<String>) -> Self {
        let exception_type = exception_type.into();
        self.exception_type = (!exception_type.is_empty()).then_some(exception_type);
        self
    }

    /// Adds one extra-info entry without interpreting its contents.
    pub fn with_extra_info(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_info.insert(key.into(), value.into());
        self
    }

    /// Sets the server's explicit invalidation signal.
    pub fn with_invalidation(mut self, must_invalidate: bool) -> Self {
        self.must_invalidate = must_invalidate;
        self
    }

    /// Returns the original, potentially sensitive message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns a typed view, or `None` for a missing or empty wire field.
    ///
    /// Unknown values borrow the original text without changing stored metadata.
    pub fn exception_type(&self) -> Option<ExceptionType<'_>> {
        self.exception_type_raw().map(ExceptionType::from)
    }

    /// Returns the exact type text, including recognized aliases and unknown names.
    ///
    /// Codecs use this to preserve wire data. The text may be sensitive.
    pub fn exception_type_raw(&self) -> Option<&str> {
        self.exception_type.as_deref()
    }

    /// Returns all extra information, including unknown keys and values.
    pub fn extra_info(&self) -> &BTreeMap<String, String> {
        &self.extra_info
    }

    /// Returns the optional typed subtype; it is not a universal error code.
    ///
    /// Classification does not move or duplicate the extra-info entry.
    pub fn error_subtype(&self) -> Option<ErrorSubtype<'_>> {
        self.error_subtype_raw().map(ErrorSubtype::from)
    }

    /// Returns the original subtype text without interpreting or sanitizing it.
    ///
    /// Missing metadata is `None`; present unrecognized values remain available.
    pub fn error_subtype_raw(&self) -> Option<&str> {
        self.extra_info.get("error_subtype").map(String::as_str)
    }

    /// Returns the server's invalidation flag without inferred classifications.
    pub fn must_invalidate(&self) -> bool {
        self.must_invalidate
    }

    /// Whether the affected session must be retired, never whether a query may be replayed.
    ///
    /// Explicit invalidation and Internal/Fatal types take precedence. Legacy
    /// prefixes and exact missing-session messages retain conservative recovery
    /// for older servers. Neither text nor metadata proves a statement did not run.
    pub fn is_fatal(&self) -> bool {
        self.must_invalidate
            || matches!(
                self.exception_type(),
                Some(ExceptionType::Fatal | ExceptionType::Internal)
            )
            || [
                "FATAL Error:",
                "Fatal Error:",
                "INTERNAL Error:",
                "Internal Error:",
            ]
            .iter()
            .any(|prefix| self.message.starts_with(prefix))
            || super::RETIRE_CONNECTION_MESSAGES.contains(&self.message.as_str())
    }
}

impl From<String> for ServerError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for ServerError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl fmt::Debug for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerError")
            .field("must_invalidate", &self.must_invalidate)
            .finish_non_exhaustive()
    }
}

impl std::error::Error for ServerError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Typed views distinguish missing, recognized, and unknown metadata.
    #[test]
    fn typed_views_preserve_absence_aliases_and_unknown_values() {
        let missing = ServerError::new("legacy").with_exception_type("");
        assert_eq!(missing.exception_type(), None);
        assert_eq!(missing.error_subtype(), None);

        for (raw, kind) in [
            ("TransactionContext", ExceptionType::Transaction),
            ("TRANSACTION", ExceptionType::Transaction),
            ("Invalid type", ExceptionType::InvalidType),
            ("NOT IMPLEMENTED", ExceptionType::NotImplemented),
            ("CATALOG", ExceptionType::Catalog),
            ("Data Corruption", ExceptionType::DataCorruption),
            ("Resource In Use", ExceptionType::ResourceInUse),
            ("Unknown Type", ExceptionType::UnknownType),
            ("Unknown", ExceptionType::Unknown("Unknown")),
            ("Network", ExceptionType::Unknown("Network")),
            ("internal", ExceptionType::Unknown("internal")),
        ] {
            let error = ServerError::new("detail").with_exception_type(raw);
            assert_eq!(error.exception_type(), Some(kind));
            assert_eq!(error.exception_type_raw(), Some(raw));
        }
        for (raw, subtype) in [
            ("MISSING_ENTRY", ErrorSubtype::MissingEntry),
            ("ENTRY_ALREADY_EXISTS", ErrorSubtype::EntryAlreadyExists),
            ("COLUMN_NOT_FOUND", ErrorSubtype::ColumnNotFound),
            ("NO_MATCHING_FUNCTION", ErrorSubtype::NoMatchingFunction),
            ("UNSUPPORTED", ErrorSubtype::Unsupported),
            ("SYNTAX_ERROR", ErrorSubtype::SyntaxError),
            ("", ErrorSubtype::Unknown("")),
            ("missing_entry", ErrorSubtype::Unknown("missing_entry")),
        ] {
            let error = ServerError::new("detail").with_extra_info("error_subtype", raw);
            assert_eq!(error.error_subtype(), Some(subtype));
            assert_eq!(error.error_subtype_raw(), Some(raw));
            assert_eq!(error.extra_info().len(), 1);
            assert_eq!(subtype.as_str(), raw);
        }
    }

    /// Formatting a borrowed classification never discloses future metadata.
    #[test]
    fn typed_views_redact_unknown_values_without_losing_them() {
        let error = ServerError::new("detail")
            .with_exception_type("invented-sensitive-type")
            .with_extra_info("error_subtype", "invented-sensitive-subtype");
        let kind = error.exception_type().unwrap();
        let subtype = error.error_subtype().unwrap();
        assert_eq!(kind.as_str(), error.exception_type_raw().unwrap());
        assert_eq!(subtype.as_str(), error.error_subtype_raw().unwrap());
        assert!(!format!("{kind:?} {subtype:?} {error:?}").contains("invented"));
        assert_eq!(format!("{:?}", ExceptionType::Catalog), "Catalog");
        assert_eq!(format!("{:?}", ErrorSubtype::MissingEntry), "MissingEntry");
    }

    #[test]
    fn recovery_uses_metadata_and_retains_conservative_legacy_signals() {
        for error in [
            ServerError::new("unprefixed").with_exception_type("Internal"),
            ServerError::new("unprefixed").with_exception_type("FATAL"),
            ServerError::new("unprefixed")
                .with_exception_type("FutureError")
                .with_invalidation(true),
            ServerError::new("INTERNAL Error: legacy"),
            ServerError::new("Invalid connection id"),
        ] {
            assert!(error.is_fatal());
            assert!(crate::QuackError::Server(error).is_connection_fatal());
        }
        for error in [
            ServerError::new("missing table").with_exception_type("Catalog"),
            ServerError::new("future failure").with_exception_type("FutureError"),
            ServerError::new("Binder Error: quoted text\nINTERNAL Error: SQL excerpt"),
        ] {
            assert!(!error.is_fatal());
        }
    }

    #[test]
    fn unknown_metadata_is_preserved_but_debug_is_redacted() {
        let error = ServerError::new("invented-sensitive-message")
            .with_exception_type("invented-sensitive-type")
            .with_extra_info("invented-sensitive-key", "invented-sensitive-value");
        assert_eq!(
            error.exception_type(),
            Some(ExceptionType::Unknown("invented-sensitive-type"))
        );
        assert_eq!(
            error.extra_info()["invented-sensitive-key"],
            "invented-sensitive-value"
        );
        assert!(!format!("{error:?}").contains("invented"));
        assert!(!format!("{:?}", crate::QuackError::Server(error.clone())).contains("invented"));
        assert_eq!(error.to_string(), error.message());
    }
}
