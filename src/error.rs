extern crate notify;

use bincode;
use capnp;
use lmdb;
use log;
use ron;
use std::error::Error as StdError;
use std::fmt;
use std::io;
use tokio::executor::SpawnError;

#[derive(Debug)]
pub enum Error {
    Notify(notify::Error),
    IO(io::Error),
    RescanRequired,
    LMDB(lmdb::Error),
    Capnp(capnp::Error),
    NotInSchema(capnp::NotInSchema),
    BincodeError(bincode::ErrorKind),
    RonSerError(ron::ser::Error),
    RonDeError(ron::de::Error),
    SetLoggerError(log::SetLoggerError),
    UuidBytesError(uuid::BytesError),
    RecvError,
    Exit,
    ImporterError(importer::Error),
    TokioSpawnError(SpawnError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Notify(ref e) => e.description(),
            Error::IO(ref e) => e.description(),
            Error::RescanRequired => "Rescan required",
            Error::LMDB(ref e) => e.description(),
            Error::Capnp(ref e) => e.description(),
            Error::NotInSchema(ref e) => e.description(),
            Error::BincodeError(ref e) => e.description(),
            Error::RonSerError(ref e) => e.description(),
            Error::RonDeError(ref e) => e.description(),
            Error::SetLoggerError(ref e) => e.description(),
            Error::UuidBytesError(ref e) => e.description(),
            Error::RecvError => "Receive error",
            Error::Exit => "Exit",
            Error::ImporterError(ref e) => e.description(),
            Error::TokioSpawnError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::Notify(ref e) => Some(e),
            Error::IO(ref e) => Some(e),
            Error::RescanRequired => None,
            Error::LMDB(ref e) => Some(e),
            Error::Capnp(ref e) => Some(e),
            Error::NotInSchema(ref e) => Some(e),
            Error::BincodeError(ref e) => Some(e),
            Error::RonSerError(ref e) => Some(e),
            Error::RonDeError(ref e) => Some(e),
            Error::SetLoggerError(ref e) => Some(e),
            Error::UuidBytesError(ref e) => Some(e),
            Error::RecvError => None,
            Error::Exit => None,
            Error::ImporterError(ref e) => Some(e),
            Error::TokioSpawnError(ref e) => Some(e),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Notify(ref e) => e.fmt(f),
            Error::IO(ref e) => e.fmt(f),
            Error::RescanRequired => f.write_str(self.description()),
            Error::LMDB(ref e) => e.fmt(f),
            Error::Capnp(ref e) => e.fmt(f),
            Error::NotInSchema(ref e) => e.fmt(f),
            Error::BincodeError(ref e) => e.fmt(f),
            Error::RonSerError(ref e) => e.fmt(f),
            Error::RonDeError(ref e) => e.fmt(f),
            Error::SetLoggerError(ref e) => e.fmt(f),
            Error::UuidBytesError(ref e) => e.fmt(f),
            Error::RecvError => f.write_str(self.description()),
            Error::Exit => f.write_str(self.description()),
            Error::ImporterError(ref e) => e.fmt(f),
            Error::TokioSpawnError(ref e) => e.fmt(f),
        }
    }
}
impl From<notify::Error> for Error {
    fn from(err: notify::Error) -> Error {
        Error::Notify(err)
    }
}
impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}
impl From<lmdb::Error> for Error {
    fn from(err: lmdb::Error) -> Error {
        Error::LMDB(err)
    }
}
impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Error {
        Error::Capnp(err)
    }
}
impl From<capnp::NotInSchema> for Error {
    fn from(err: capnp::NotInSchema) -> Error {
        Error::NotInSchema(err)
    }
}
impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Error {
        Error::BincodeError(*err)
    }
}
impl From<ron::ser::Error> for Error {
    fn from(err: ron::ser::Error) -> Error {
        Error::RonSerError(err)
    }
}
impl From<ron::de::Error> for Error {
    fn from(err: ron::de::Error) -> Error {
        Error::RonDeError(err)
    }
}
impl From<Error> for capnp::Error {
    fn from(err: Error) -> capnp::Error {
        capnp::Error::failed(format!("{}", err))
    }
}
impl From<log::SetLoggerError> for Error {
    fn from(err: log::SetLoggerError) -> Error {
        Error::SetLoggerError(err)
    }
}
impl From<uuid::BytesError> for Error {
    fn from(err: uuid::BytesError) -> Error {
        Error::UuidBytesError(err)
    }
}
impl From<importer::Error> for Error {
    fn from(err: importer::Error) -> Error {
        Error::ImporterError(err)
    }
}
impl From<SpawnError> for Error {
    fn from(err: SpawnError) -> Error {
        Error::TokioSpawnError(err)
    }
}
