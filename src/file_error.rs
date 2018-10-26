extern crate notify;

use std::fmt;
use std::io;
use lmdb;
use capnp;
use std::error::{self, Error};

#[derive(Debug)]
pub enum FileError {
    Notify(notify::Error),
    IO(io::Error),
    RescanRequired,
    LMDB(lmdb::Error),
    Capnp(capnp::Error),
    NotInSchema(capnp::NotInSchema),
    RecvError,
    Exit,
}

impl error::Error for FileError {
    fn description(&self) -> &str {
        match *self {
            FileError::Notify(ref e) => e.description(),
            FileError::IO(ref e) => e.description(),
            FileError::RescanRequired => "Rescan required",
            FileError::LMDB(ref e) => e.description(),
            FileError::Capnp(ref e) => e.description(),
            FileError::NotInSchema(ref e) => e.description(),
            FileError::RecvError => "Receive error",
            FileError::Exit => "Exit",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            FileError::Notify(ref e) => Some(e),
            FileError::IO(ref e) => Some(e),
            FileError::RescanRequired => None,
            FileError::LMDB(ref e) => Some(e),
            FileError::Capnp(ref e) => Some(e),
            FileError::NotInSchema(ref e) => Some(e),
            FileError::RecvError => None,
            FileError::Exit => None,
        }
    }
}
impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FileError::Notify(ref e) => e.fmt(f),
            FileError::IO(ref e) => e.fmt(f),
            FileError::RescanRequired => f.write_str(self.description()),
            FileError::LMDB(ref e) => e.fmt(f),
            FileError::Capnp(ref e) => e.fmt(f),
            FileError::NotInSchema(ref e) => e.fmt(f),
            FileError::RecvError => f.write_str(self.description()),
            FileError::Exit => f.write_str(self.description()),
        }
    }
}
impl From<notify::Error> for FileError {
    fn from(err: notify::Error) -> FileError {
        FileError::Notify(err)
    }
}
impl From<io::Error> for FileError {
    fn from(err: io::Error) -> FileError {
        FileError::IO(err)
    }
}
impl From<lmdb::Error> for FileError {
    fn from(err: lmdb::Error) -> FileError {
        FileError::LMDB(err)
    }
}
impl From<capnp::Error> for FileError {
    fn from(err: capnp::Error) -> FileError {
        FileError::Capnp(err)
    }
}
impl From<capnp::NotInSchema> for FileError {
    fn from(err: capnp::NotInSchema) -> FileError {
        FileError::NotInSchema(err)
    }
}