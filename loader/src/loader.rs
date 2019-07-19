use crate::{AssetTypeId, AssetUuid};
use crossbeam_channel::Sender;
use std::{error::Error, sync::Arc};

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub struct LoadHandle(pub(crate) u64);

pub(crate) enum HandleOp {
    LoadError(LoadHandle, Box<dyn Error + Send>),
    LoadComplete(LoadHandle),
    LoadDrop(LoadHandle),
}

pub struct AssetLoadOp {
    sender: Option<Arc<Sender<HandleOp>>>,
    handle: LoadHandle,
}
impl AssetLoadOp {
    pub(crate) fn new(sender: Arc<Sender<HandleOp>>, handle: LoadHandle) -> Self {
        Self {
            sender: Some(sender),
            handle,
        }
    }
    pub fn complete(mut self) {
        let _ = self
            .sender
            .as_ref()
            .unwrap()
            .send(HandleOp::LoadComplete(self.handle));
        self.sender = None;
    }
    pub fn error<E: Error + 'static + Send>(mut self, error: E) {
        let _ = self
            .sender
            .as_ref()
            .unwrap()
            .send(HandleOp::LoadError(self.handle, Box::new(error)));
        self.sender = None;
    }
}
impl Drop for AssetLoadOp {
    fn drop(&mut self) {
        if let Some(ref sender) = self.sender {
            let _ = sender.send(HandleOp::LoadDrop(self.handle));
        }
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
        data: &dyn AsRef<[u8]>, //TODO: make it a slice
        loader_handle: &LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error>>;
    fn commit_asset_version(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: &Self::HandleType,
        loader_handle: &LoadHandle,
        version: u32,
    );
    fn free(
        &self,
        asset_type: &AssetTypeId,
        storage_handle: Self::HandleType,
        loader_handle: LoadHandle,
    );
}

#[derive(Debug)]
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
    // TODO: this type is always (), should be removed
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
