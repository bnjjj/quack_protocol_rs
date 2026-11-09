//! Borrowed classifications of native server errors without changing wire data.

use std::fmt;

/// Defines a wire-name classification with a redacted, lossless unknown case.
///
/// Exception types and subtypes share this mapping contract, while retaining
/// separate enums so callers cannot accidentally compare the two domains.
macro_rules! error_classification {
    ($(#[$doc:meta])* pub enum $name:ident { $( $(#[$variant_doc:meta])* $variant:ident => $wire:literal $(| $alias:literal)*, )* }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Eq, PartialEq)]
        #[non_exhaustive]
        pub enum $name<'a> {
            $( $(#[$variant_doc])* $variant, )*
            /// An unrecognized value, borrowed unchanged from the server metadata.
            ///
            /// This text is untrusted and may be sensitive; `Debug` redacts it.
            Unknown(&'a str),
        }

        impl<'a> From<&'a str> for $name<'a> {
            fn from(value: &'a str) -> Self {
                match value {
                    $( $wire $(| $alias)* => Self::$variant, )*
                    unknown => Self::Unknown(unknown),
                }
            }
        }

        impl<'a> $name<'a> {
            /// Returns the canonical wire name, or the original unknown text.
            ///
            /// Callers must sanitize unknown text before logging or forwarding it.
            /// Use `ServerError`'s raw accessors when preserving a recognized alias.
            pub fn as_str(self) -> &'a str {
                match self {
                    $( Self::$variant => $wire, )*
                    Self::Unknown(value) => value,
                }
            }
        }

        impl fmt::Debug for $name<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $( Self::$variant => f.write_str(stringify!($variant)), )*
                    Self::Unknown(_) => f.debug_tuple("Unknown").field(&"[redacted]").finish(),
                }
            }
        }
    };
}

error_classification! {
    /// A known DuckDB exception category, or an unchanged future wire value.
    ///
    /// Returned by [`crate::ServerError::exception_type`]. Known names follow
    /// DuckDB 1.5/2.0's `ExceptionTypeToString` mapping, with explicit legacy
    /// aliases. This classification does not authorize disclosure or replay.
    ///
    /// Upstream's `NETWORK` enum has no wire-name mapping and renders `Unknown`;
    /// it cannot be recovered as a distinct category from that string.
    /// See <https://github.com/duckdb/duckdb/blob/v2.0.0/src/common/exception.cpp>.
    pub enum ExceptionType {
        /// Invalid or unspecified upstream exception category.
        Invalid => "Invalid",
        /// A value is outside the supported range.
        OutOfRange => "Out of Range",
        /// A conversion or cast failed.
        Conversion => "Conversion" | "CONVERSION",
        /// DuckDB could not recognize a data type.
        UnknownType => "Unknown Type",
        /// A decimal operation failed.
        Decimal => "Decimal",
        /// Data types do not match.
        MismatchType => "Mismatch Type",
        /// Division by zero.
        DivideByZero => "Divide by Zero",
        /// An object exceeds its size limit.
        ObjectSize => "Object Size",
        /// A type is incompatible with an operation.
        InvalidType => "Invalid type",
        /// Serialization or deserialization failed.
        Serialization => "Serialization",
        /// A transaction operation failed.
        Transaction => "TransactionContext" | "Transaction" | "TRANSACTION",
        /// An operation is not implemented.
        NotImplemented => "Not implemented" | "Not Implemented" | "NOT IMPLEMENTED",
        /// An expression operation failed.
        Expression => "Expression",
        /// A catalog lookup or modification failed.
        Catalog => "Catalog" | "CATALOG",
        /// SQL parsing failed.
        Parser => "Parser" | "PARSER",
        /// SQL binding failed.
        Binder => "Binder" | "BINDER",
        /// Query planning failed.
        Planner => "Planner",
        /// Query scheduling failed.
        Scheduler => "Scheduler",
        /// Query execution failed.
        Executor => "Executor",
        /// A constraint was violated.
        Constraint => "Constraint" | "CONSTRAINT",
        /// An index operation failed.
        Index => "Index",
        /// A statistics operation failed.
        Stat => "Stat",
        /// A connection operation failed.
        Connection => "Connection" | "CONNECTION",
        /// A syntax operation failed.
        Syntax => "Syntax",
        /// A settings operation failed.
        Settings => "Settings",
        /// Query optimization failed.
        Optimizer => "Optimizer",
        /// An unexpected null pointer was encountered.
        NullPointer => "NullPointer",
        /// An input/output operation failed.
        Io => "IO",
        /// The operation was interrupted.
        Interrupt => "INTERRUPT" | "Interrupt",
        /// DuckDB reported a fatal failure.
        Fatal => "FATAL" | "Fatal",
        /// DuckDB reported an internal failure.
        Internal => "INTERNAL" | "Internal",
        /// Input or arguments are invalid.
        InvalidInput => "Invalid Input" | "INVALID INPUT",
        /// Memory allocation failed.
        OutOfMemory => "Out of Memory" | "OUT OF MEMORY",
        /// Permission was denied.
        Permission => "Permission" | "PERMISSION",
        /// Parameter types could not be resolved.
        ParameterNotResolved => "Parameter Not Resolved",
        /// Parameters are not allowed in this context.
        ParameterNotAllowed => "Parameter Not Allowed",
        /// An object dependency prevents an operation.
        Dependency => "Dependency",
        /// A required extension is missing.
        MissingExtension => "Missing Extension",
        /// An HTTP operation failed.
        Http => "HTTP",
        /// Automatic extension loading failed.
        ExtensionAutoloading => "Extension Autoloading",
        /// A sequence operation failed.
        Sequence => "Sequence",
        /// Configuration is invalid or incomplete.
        InvalidConfiguration => "Invalid Configuration" | "INVALID CONFIGURATION",
        /// Persistent storage contains corrupted data (DuckDB 2.0+).
        DataCorruption => "Data Corruption",
        /// A required resource is already in use (DuckDB 2.0+).
        ResourceInUse => "Resource In Use",
    }
}

error_classification! {
    /// An optional error detail recognized from DuckDB's extra-info map.
    ///
    /// Returned by [`crate::ServerError::error_subtype`]. DuckDB does not define
    /// a closed subtype enum: extensions and future versions may add values.
    /// Interpret known subtypes together with their exception category.
    pub enum ErrorSubtype {
        /// A catalog entry does not exist (Catalog).
        MissingEntry => "MISSING_ENTRY",
        /// A catalog entry already exists (Catalog).
        EntryAlreadyExists => "ENTRY_ALREADY_EXISTS",
        /// A referenced column could not be bound (Binder).
        ColumnNotFound => "COLUMN_NOT_FOUND",
        /// No function overload matches the supplied arguments (Binder).
        NoMatchingFunction => "NO_MATCHING_FUNCTION",
        /// A binding operation is unsupported (Binder).
        Unsupported => "UNSUPPORTED",
        /// SQL syntax is invalid (Parser).
        SyntaxError => "SYNTAX_ERROR",
    }
}
