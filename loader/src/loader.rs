use crate::handle::Handle;
use crate::AssetUuid;
use serde_dyn::TypeUuid;
use std::error::Error;

pub trait AssetLoad {
    fn get_asset<T: TypeUuid>() -> Option<Handle<T>>;
}

pub trait AssetStorage {
    fn allocate(&self) -> u32;
    fn update_asset(&self, handle: u32, data: &AsRef<[u8]>) -> Result<(), Box<dyn Error>>;
    fn free(&self, handle: u32);
}

pub struct AssetType {
    pub uuid: u128,
    pub create_storage: fn() -> Box<dyn AssetStorage>,
}
inventory::collect!(AssetType);

pub trait Loader {
    type LoadOp: AssetLoad;
    fn add_asset_ref(&mut self, id: AssetUuid) -> Self::LoadOp;
    fn get_asset_load(&self, id: &AssetUuid) -> Option<Self::LoadOp>;
    fn decrease_asset_ref(&mut self, id: AssetUuid);
    fn process(&mut self);
}
