use crate::{AssetTypeId, AssetUuid};
use std::error::Error;

#[derive(Copy, Clone, PartialEq)]
pub struct LoadHandle(pub(crate) u64);

pub trait AssetStorage {
    type HandleType;
    fn allocate(&self, asset_type: &AssetTypeId, id: &AssetUuid, loader_handle: LoadHandle) -> Self::HandleType;
    fn update_asset(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: &Self::HandleType,
        data: &dyn AsRef<[u8]>,
        loader_handle: LoadHandle,
    ) -> Result<(), Box<dyn Error>>;
    fn is_loaded(&self, asset_type: &AssetTypeId, storage_handle: &Self::HandleType, loader_handle: LoadHandle) -> bool;
    fn free(&self, asset_type: &AssetTypeId, storage_handle: Self::HandleType, loader_handle: LoadHandle);
}

pub struct LoadInfo {
    pub asset_id: AssetUuid,
    pub refs: u32,
}

pub trait Loader {
    type HandleType;
    fn load(&mut self, id: AssetUuid) -> LoadHandle;
    fn unload(&mut self, load: LoadHandle);
    fn get_asset(&self, load: LoadHandle) -> Option<(AssetTypeId, Self::HandleType, LoadHandle)>;
    fn get_load(&self, id: AssetUuid) -> Option<LoadHandle>;
    fn get_load_info(&self, load: LoadHandle) -> Option<LoadInfo>;
    fn process(
        &mut self,
        asset_storage: &dyn AssetStorage<HandleType = Self::HandleType>,
    ) -> Result<(), Box<dyn Error>>;
}
