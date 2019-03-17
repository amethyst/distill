use crate::{AssetTypeId, AssetUuid};
use std::error::Error;

#[derive(Copy, Clone, PartialEq)]
pub struct LoaderHandle(pub(crate) u64);

pub trait AssetStorage {
    type HandleType;
    fn allocate(&self, asset_type: &AssetTypeId, id: &AssetUuid, loader_handle: LoaderHandle) -> Self::HandleType;
    fn update_asset(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: &Self::HandleType,
        data: &dyn AsRef<[u8]>,
        loader_handle: LoaderHandle,
    ) -> Result<(), Box<dyn Error>>;
    fn is_loaded(&self, asset_type: &AssetTypeId, storage_handle: &Self::HandleType, loader_handle: LoaderHandle) -> bool;
    fn free(&self, asset_type: &AssetTypeId, storage_handle: Self::HandleType, loader_handle: LoaderHandle);
}

pub trait ComputedAsset {
    fn build(&self);
    fn update(&self);
}

pub trait Loader {
    type HandleType;
    fn add_asset_ref(&mut self, id: AssetUuid);
    fn decrease_asset_ref(&mut self, id: AssetUuid);
    fn get_asset(&self, id: AssetUuid) -> Option<(AssetTypeId, Self::HandleType, LoaderHandle)>;
    fn process(
        &mut self,
        asset_storage: &dyn AssetStorage<HandleType = Self::HandleType>,
    ) -> Result<(), Box<dyn Error>>;
}
