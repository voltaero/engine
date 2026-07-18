use std::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    NotSupported,
    InvalidArgument,
    IOError,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn new(message: String) -> Error {
        Error { message }
    }

    pub fn into_string(self) -> String {
        self.into()
    }

    /// Parse corresponding [`ErrorKind`] from error message.
    pub fn kind(&self) -> ErrorKind {
        match self.message.split(':').next().unwrap_or("") {
            "NotFound" => ErrorKind::NotFound,
            "Not implemented" => ErrorKind::NotSupported,
            "Invalid argument" => ErrorKind::InvalidArgument,
            "IO error" => ErrorKind::IOError,
            _ => ErrorKind::Unknown,
        }
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

impl std::error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}
