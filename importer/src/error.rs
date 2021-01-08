use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    IoError(std::io::Error),
    Uuid(uuid::Error),
    ErasedSerde(erased_serde::Error),
    #[cfg(feature = "serde_importers")]
    RonDe(ron::de::Error),
    Boxed(Box<dyn std::error::Error + Send>),
    ExportUnsupported,
    Custom(String),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Error::IoError(ref e) => Some(e),
            Error::Uuid(ref e) => Some(e),
            Error::ErasedSerde(ref e) => Some(e),
            #[cfg(feature = "serde_importers")]
            Error::RonDe(ref e) => Some(e),
            Error::Boxed(ref e) => e.source(),
            Error::ExportUnsupported => None,
            Error::Custom(_) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::Uuid(ref e) => e.fmt(f),
            Error::IoError(ref e) => e.fmt(f),
            Error::ErasedSerde(ref e) => e.fmt(f),
            #[cfg(feature = "serde_importers")]
            Error::RonDe(ref e) => e.fmt(f),
            Error::Boxed(ref e) => e.fmt(f),
            Error::ExportUnsupported => write!(f, "{:?}", self),
            Error::Custom(ref e) => write!(f, "{}", e),
        }
    }
}

impl From<uuid::Error> for Error {
    fn from(err: uuid::Error) -> Error {
        Error::Uuid(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}
impl From<erased_serde::Error> for Error {
    fn from(err: erased_serde::Error) -> Error {
        Error::ErasedSerde(err)
    }
}

#[cfg(feature = "serde_importers")]
impl From<ron::de::Error> for Error {
    fn from(err: ron::de::Error) -> Error {
        Error::RonDe(err)
    }
}

impl From<Box<dyn std::error::Error + Send>> for Error {
    fn from(err: Box<dyn std::error::Error + Send>) -> Error {
        Error::Boxed(err)
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::Custom(err)
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Error {
        Error::Custom(err.into())
    }
}
