mod schemas;
pub use schemas::data_capnp;
pub use schemas::service_capnp;
impl ::std::fmt::Debug for data_capnp::FileState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            data::FileState::Exists => {
                write!(f, "FileState::Exists")?;
            }
            data::FileState::Deleted => {
                write!(f, "FileState::Deleted")?;
            }
        }
        Ok(())
    }
}

impl From<atelier_core::CompressionType> for data_capnp::CompressionType {
    fn from(c: atelier_core::CompressionType) -> Self {
        match c {
            atelier_core::CompressionType::None => Self::None,
            atelier_core::CompressionType::Lz4 => Self::Lz4,
        }
    }
}

impl From<data_capnp::CompressionType> for atelier_core::CompressionType {
    fn from(c: data_capnp::CompressionType) -> Self {
        match c {
            data_capnp::CompressionType::None => Self::None,
            data_capnp::CompressionType::Lz4 => Self::Lz4,
        }
    }
}

pub use crate::{data_capnp as data, service_capnp as service};
