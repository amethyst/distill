mod asset_data;
mod handle;
pub mod loader;
#[cfg(feature = "rpc_loader")]
pub mod rpc_loader;

pub use crate::loader::{Loader, AssetStorage, AssetType};
pub use inventory;
type AssetUuid = [u8; 16];

pub type DefaultLoader = rpc_loader::RpcLoader;
