#![warn(rust_2018_idioms, rust_2018_compatibility)]
#![feature(fnbox)]

mod asset_data;
pub mod loader;
#[cfg(feature = "rpc_loader")]
pub mod rpc_loader;
#[cfg(feature = "rpc_loader")]
mod rpc_state;

pub use crate::loader::{AssetStorage, LoadHandle, LoadStatus, Loader};
pub use slotmap;
pub type AssetUuid = [u8; 16];
pub type AssetTypeId = [u8; 16];
