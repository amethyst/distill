#![warn(rust_2018_idioms, rust_2018_compatibility)]

mod asset_data;
mod loader;
#[cfg(feature = "rpc_loader")]
pub mod rpc_loader;
#[cfg(feature = "rpc_loader")]
mod rpc_state;

pub use crate::loader::{AssetLoadOp, AssetStorage, LoadHandle, LoadStatus, Loader};
pub use type_uuid::{TypeUuid, TypeUuidDynamic};

/// A universally unique identifier for an asset.
/// An asset can be an instance of any Rust type that implements 
/// [type_uuid::TypeUuid] + [serde::Serialize] + [Send].
pub type AssetUuid = [u8; 16];
/// UUID of an asset's Rust type. Produced by [type_uuid::TypeUuid::UUID].
pub type AssetTypeId = [u8; 16];
