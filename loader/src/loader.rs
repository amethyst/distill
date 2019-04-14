use crate::{AssetTypeId, AssetUuid};
use crossbeam_channel::Sender;
use std::{error::Error, sync::Arc};

#[derive(Copy, Clone, PartialEq, Debug, Hash)]
pub struct LoadHandle(pub(crate) u64);

pub(crate) enum HandleOp {
    LoadError(LoadHandle, Box<dyn Error + Send>),
    LoadComplete(LoadHandle),
    LoadDrop(LoadHandle),
}

pub struct AssetLoadOp {
    sender: Arc<Sender<HandleOp>>,
    handle: LoadHandle,
}
impl AssetLoadOp {
    pub(crate) fn new(sender: Arc<Sender<HandleOp>>, handle: LoadHandle) -> Self {
        Self { sender, handle }
    }
    pub fn complete(self) {
        let _ = self.sender.send(HandleOp::LoadComplete(self.handle));
    }
    pub fn error<E: Error + 'static + Send>(self, error: E) {
        let _ = self
            .sender
            .send(HandleOp::LoadError(self.handle, Box::new(error)));
    }
}
impl Drop for AssetLoadOp {
    fn drop(&mut self) {
        let _ = self.sender.send(HandleOp::LoadDrop(self.handle));
    }
}

pub trait AssetStorage {
    type HandleType;
    fn allocate(
        &self,
        asset_type: &AssetTypeId,
        id: &AssetUuid,
        loader_handle: &LoadHandle,
    ) -> Self::HandleType;
    fn update_asset(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: &Self::HandleType,
        data: &dyn AsRef<[u8]>,
        loader_handle: &LoadHandle,
    ) -> Result<(), Box<dyn Error>>;
    fn is_loaded(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: &Self::HandleType,
        loader_handle: &LoadHandle,
    ) -> bool;
    fn free(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: Self::HandleType,
        loader_handle: LoadHandle,
    );
}

pub enum LoadStatus {
    NotRequested,
    Loading,
    Loaded,
    Unloading,
    DoesNotExist,
    Error(Box<dyn Error>),
}

pub struct LoadInfo {
    pub asset_id: AssetUuid,
    pub refs: u32,
}

pub trait Loader {
    type HandleType;
    fn add_ref(&self, id: AssetUuid) -> LoadHandle;
    fn remove_ref(&self, id: &LoadHandle);
    fn get_asset(&self, load: &LoadHandle) -> Option<(AssetTypeId, Self::HandleType, LoadHandle)>;
    fn get_load(&self, id: AssetUuid) -> Option<LoadHandle>;
    fn get_load_info(&self, load: &LoadHandle) -> Option<LoadInfo>;
    fn get_load_status(&self, load: &LoadHandle) -> LoadStatus;
    fn process(
        &mut self,
        asset_storage: &dyn AssetStorage<HandleType = Self::HandleType>,
    ) -> Result<(), Box<dyn Error>>;
}
