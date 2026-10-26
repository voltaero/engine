use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    NotFound,
    NotSupported,
    InvalidArgument,
    IOError,
    Unknown,
}

/// An engine error. The message is a `Cow` so the common static messages
/// (`"TaskNotFound"`, `"LeaseTokenMismatch"`, ...) never allocate, and cloning
/// one error into every item of a batch is free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    message: Cow<'static, str>,
}

impl Error {
    pub fn new(message: String) -> Error {
        Error {
            kind: ErrorKind::Unknown,
            message: Cow::Owned(message),
        }
    }

    pub fn with_kind(kind: ErrorKind, message: impl Into<Cow<'static, str>>) -> Error {
        Error {
            kind,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<Cow<'static, str>>) -> Error {
        Self::with_kind(ErrorKind::NotFound, message)
    }

    pub fn invalid_argument(message: impl Into<Cow<'static, str>>) -> Error {
        Self::with_kind(ErrorKind::InvalidArgument, message)
    }

    pub fn io_error(message: impl Into<Cow<'static, str>>) -> Error {
        Self::with_kind(ErrorKind::IOError, message)
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
        e.message.into_owned()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}
