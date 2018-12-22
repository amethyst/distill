use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    AmethystError(amethyst::Error),
    AmethystAssetsError(amethyst::assets::Error),
    RonDeError(ron::de::Error),
    BincodeError(bincode::ErrorKind),
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IoError(ref e) => e.description(),
            Error::AmethystError(ref e) => e.description(),
            Error::AmethystAssetsError(ref e) => e.description(),
            Error::RonDeError(ref e) => e.description(),
            Error::BincodeError(ref e) => e.description(),
        }
    }
    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IoError(ref e) => Some(e),
            Error::AmethystError(ref e) => Some(e),
            Error::AmethystAssetsError(ref e) => Some(e),
            Error::RonDeError(ref e) => Some(e),
            Error::BincodeError(ref e) => Some(e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IoError(ref e) => e.fmt(f),
            Error::AmethystError(ref e) => e.fmt(f),
            Error::AmethystAssetsError(ref e) => e.fmt(f),
            Error::RonDeError(ref e) => e.fmt(f),
            Error::BincodeError(ref e) => e.fmt(f),
        }
    }
}

impl From<amethyst::Error> for Error {
    fn from(err: amethyst::Error) -> Error {
        Error::AmethystError(err)
    }
}
impl From<amethyst::assets::Error> for Error {
    fn from(err: amethyst::assets::Error) -> Error {
        Error::AmethystAssetsError(err)
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
