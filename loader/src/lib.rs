#![warn(rust_2018_idioms, rust_2018_compatibility)]

mod asset_data;
pub mod handle;
mod loader;
#[cfg(feature = "rpc_loader")]
pub mod rpc_loader;
#[cfg(feature = "rpc_loader")]
mod rpc_state;

pub use crate::loader::{
    AssetLoadOp, AssetStorage, LoadHandle, LoadInfo, LoadStatus, Loader, LoaderInfoProvider,
};
pub use atelier_core::{asset_uuid, AssetRef, AssetTypeId, AssetUuid};
pub use crossbeam_channel;
pub use type_uuid::{TypeUuid, TypeUuidDynamic};
