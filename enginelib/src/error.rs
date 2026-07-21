use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    NotFound,
    NotSupported,
    InvalidArgument,
    IOError,
    Overloaded,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub fn new(message: String) -> Error {
        Error {
            kind: ErrorKind::Unknown,
            message,
        }
    }

    pub fn with_kind(kind: ErrorKind, message: impl Into<String>) -> Error {
        Error {
            kind,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Error {
        Self::with_kind(ErrorKind::NotFound, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Error {
        Self::with_kind(ErrorKind::InvalidArgument, message)
    }

    pub fn io_error(message: impl Into<String>) -> Error {
        Self::with_kind(ErrorKind::IOError, message)
    }

    pub fn overloaded(message: impl Into<String>) -> Error {
        Self::with_kind(ErrorKind::Overloaded, message)
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl AsRef<str> for Error {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.message
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}
