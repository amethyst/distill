use proc_macro_hack::proc_macro_hack;

#[proc_macro_hack]
pub use asset_uuid::asset_uuid;
pub mod utils;

/// A universally unique identifier for an asset.
/// An asset can be an instance of any Rust type that implements
/// [type_uuid::TypeUuid] + [serde::Serialize] + [Send].
pub type AssetUuid = [u8; 16];
/// UUID of an asset's Rust type. Produced by [type_uuid::TypeUuid::UUID].
pub type AssetTypeId = [u8; 16];
