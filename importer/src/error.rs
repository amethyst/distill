use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    RonDeError(ron::de::Error),
    BincodeError(bincode::ErrorKind),
    Boxed(Box<dyn std::error::Error + Send>),
    ExportUnsupported,
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IoError(ref e) => e.description(),
            Error::RonDeError(ref e) => e.description(),
            Error::BincodeError(ref e) => e.description(),
            Error::Boxed(ref e) => e.description(),
            Error::ExportUnsupported => "Exporting not supported by this Importer",
        }
    }
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Error::IoError(ref e) => Some(e),
            Error::RonDeError(ref e) => Some(e),
            Error::BincodeError(ref e) => Some(e),
            Error::Boxed(ref e) => e.source(),
            Error::ExportUnsupported => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::IoError(ref e) => e.fmt(f),
            Error::RonDeError(ref e) => e.fmt(f),
            Error::BincodeError(ref e) => e.fmt(f),
            Error::Boxed(ref e) => e.fmt(f),
            Error::ExportUnsupported => {
                write!(f, "{}", <Self as std::error::Error>::description(self))
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}
impl From<ron::de::Error> for Error {
    fn from(err: ron::de::Error) -> Error {
        Error::RonDeError(err)
    }
}
impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Error {
        Error::BincodeError(*err)
    }
}
impl From<Box<dyn std::error::Error + Send>> for Error {
    fn from(err: Box<dyn std::error::Error + Send>) -> Error {
        Error::Boxed(err)
    }
}
