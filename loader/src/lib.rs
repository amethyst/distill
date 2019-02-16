#![warn(rust_2018_idioms, rust_2018_compatibility)]

mod asset_data;
pub mod loader;
#[cfg(feature = "rpc_loader")]
pub mod rpc_loader;

pub use crate::loader::{Loader, AssetStorage};
pub type AssetUuid = [u8; 16];
pub type AssetTypeId = [u8; 16];
