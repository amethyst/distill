extern crate capnp;

#[allow(clippy::all)]
#[allow(dead_code)]
pub mod data_capnp {
    include!(concat!(env!("OUT_DIR"), "/schemas/data_capnp.rs"));
}
#[allow(clippy::all)]
#[allow(dead_code)]
pub mod service_capnp {
    include!(concat!(env!("OUT_DIR"), "/schemas/service_capnp.rs"));
}
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

pub use data_capnp as data;
pub use service_capnp as service;
