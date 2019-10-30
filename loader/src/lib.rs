#![warn(rust_2018_idioms, rust_2018_compatibility)]

mod asset_data;
mod loader;
#[cfg(feature = "rpc_loader")]
pub mod rpc_loader;
#[cfg(feature = "rpc_loader")]
mod rpc_state;

pub use crate::loader::{
    AssetLoadOp, AssetStorage, LoadHandle, LoadStatus, Loader, LoaderInfoProvider,
};
pub use atelier_core::{AssetRef, AssetTypeId, AssetUuid};
pub use type_uuid::{TypeUuid, TypeUuidDynamic};
