use crate::{AssetUuid, AssetTypeId};
use serde_dyn::TypeUuid;
use std::error::Error;

pub trait AssetStorage {
    type HandleType;
    fn allocate(&self, asset_type: &AssetTypeId, id: &AssetUuid) -> Self::HandleType;
    fn update_asset(&self, asset_type: &AssetTypeId, handle: &Self::HandleType, data: &AsRef<[u8]>) -> Result<(), Box<dyn Error>>;
    fn free(&self, asset_type: &AssetTypeId, handle: Self::HandleType);
}

pub trait Loader {
    type LoadOp;
    type HandleType;
    fn add_asset_ref(&mut self, id: AssetUuid) -> Self::LoadOp;
    fn get_asset_load(&self, id: &AssetUuid) -> Option<Self::LoadOp>;
    fn decrease_asset_ref(&mut self, id: AssetUuid);
    fn get_asset(&self, load: &Self::LoadOp) -> Option<(AssetTypeId, Self::HandleType)>;
    fn process(&mut self, asset_storage: &dyn AssetStorage<HandleType = Self::HandleType>) -> Result<(), Box<dyn Error>>;
}
