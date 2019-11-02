use atelier_core::{AssetRef, AssetTypeId, AssetUuid};
use crossbeam_channel::Sender;
use std::{error::Error, sync::Arc};

/// Loading ID allocated by `atelier-assets` to track loading of a particular asset.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub struct LoadHandle(pub u64);

pub(crate) enum HandleOp {
    LoadError(LoadHandle, Box<dyn Error + Send>),
    LoadComplete(LoadHandle),
    LoadDrop(LoadHandle),
}

/// Type that allows the downstream asset storage implementation to signal that this asset is
/// loaded.
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

    /// Signals that this load operation has completed succesfully.
    pub fn complete(mut self) {
        let _ = self
            .sender
            .as_ref()
            .unwrap()
            .send(HandleOp::LoadComplete(self.handle));
        self.sender = None;
    }

    /// Signals that this load operation has completed with an error.
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

/// Storage for all assets of all asset types.
///
/// Consumers are expected to provide the implementation for this, as this is the bridge between
/// `atelier-assets` and the application.
pub trait AssetStorage {
    /// Updates the backing data of an asset.
    ///
    /// An example usage of this is when a texture such as "player.png" changes while the
    /// application is running. The asset ID is the same, but the underlying pixel data can differ.
    ///
    /// # Parameters
    ///
    /// * `loader`: Loader implementation calling this function.
    /// * `asset_type_id`: UUID of the asset type.
    /// * `data`: The updated asset byte data.
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of a particular asset.
    /// * `load_op`: Allows the loading implementation to signal when loading is done / errors.
    /// * `version`: Runtime load version of this asset, increments each time the asset is updated.
    fn update_asset(
        &self,
        loader_info: &dyn LoaderInfoProvider,
        asset_type_id: &AssetTypeId,
        data: &[u8],
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error>>;

    /// Commits the specified asset version as loaded and ready to use.
    ///
    /// # Parameters
    ///
    /// * `asset_type_id`: UUID of the asset type.
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of a particular asset.
    /// * `version`: Runtime load version of this asset, increments each time the asset is updated.
    fn commit_asset_version(&self, asset_type: &AssetTypeId, load_handle: LoadHandle, version: u32);

    /// Frees the asset identified by the load handle.
    ///
    /// # Parameters
    ///
    /// * `asset_type_id`: UUID of the asset type.
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of a particular asset.
    fn free(&self, asset_type_id: &AssetTypeId, load_handle: LoadHandle);
}

/// Asset loading status.
#[derive(Debug)]
pub enum LoadStatus {
    /// There is no request for the asset to be loaded.
    NotRequested,
    /// The asset is being loaded.
    Loading,
    /// The asset is loaded.
    Loaded,
    /// The asset is being unloaded.
    Unloading,
    /// The asset does not exist.
    DoesNotExist,
    /// There was an error during loading / unloading of the asset.
    Error(Box<dyn Error>),
}

/// Indicates the number of references there are to an asset.
///
/// **Note:** The information is true at the time the `LoadInfo` is retrieved. The actual number of
/// references may change.
pub struct LoadInfo {
    /// UUID of the asset.
    pub asset_id: AssetUuid,
    /// Number of references to the asset.
    pub refs: u32,
}

/// Tracks asset loading, allocating a load handle for each asset.
///
/// This is implemented by the `atelier-assets` [`RpcLoader`]. Consumers of `atelier-assets` should
/// delegate to `T: atelier_assets::Loader` for the asset loading implementation instead of
/// implementing this trait themselves.
pub trait Loader {
    /// Adds a reference to an asset and returns its [`LoadHandle`].
    ///
    /// If the asset is already loaded, this returns the existing [`LoadHandle`]. If it is not
    /// loaded, this allocates a new [`LoadHandle`] and returns that.
    ///
    /// # Parameters
    ///
    /// * `id`: UUID of the asset.
    fn add_ref(&self, id: AssetUuid) -> LoadHandle;

    /// Removes a reference to an asset.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    fn remove_ref(&self, load_handle: LoadHandle);

    /// Returns the `AssetType` UUID and load handle if the asset is loaded.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    fn get_asset(&self, load: LoadHandle) -> Option<(AssetTypeId, LoadHandle)>;

    /// Returns the load handle for the asset with the given UUID, if present.
    ///
    /// This will only return `Some(..)` if there has been a previous call to [`Loader::add_ref`].
    ///
    /// # Parameters
    ///
    /// * `id`: UUID of the asset.
    fn get_load(&self, id: AssetUuid) -> Option<LoadHandle>;

    /// Returns the number of references to an asset.
    ///
    /// **Note:** The information is true at the time the `LoadInfo` is retrieved. The actual number
    /// of references may change.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    fn get_load_info(&self, load: LoadHandle) -> Option<LoadInfo>;

    /// Returns the asset load status.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    fn get_load_status(&self, load: LoadHandle) -> LoadStatus;

    /// Processes pending load operations.
    ///
    /// Load operations include:
    ///
    /// * Requesting asset metadata.
    /// * Requesting asset data.
    /// * Committing completed [`AssetLoadOp`]s.
    /// * Updating the [`LoadStatus`]es of assets.
    ///
    /// # Parameters
    ///
    /// * `asset_storage`: Storage for all assets of all asset types.
    fn process(&mut self, asset_storage: &dyn AssetStorage) -> Result<(), Box<dyn Error>>;
}

/// Provides information about mappings between `AssetUuid` and `LoadHandle`.
/// Intended to be used for `Handle` serde.
pub trait LoaderInfoProvider {
    /// Returns the load handle for the asset with the given UUID, if present.
    ///
    /// This will only return `Some(..)` if there has been a previous call to [`Loader::add_ref`].
    ///
    /// # Parameters
    ///
    /// * `id`: UUID of the asset.
    fn get_load_handle(&self, asset_ref: &AssetRef) -> Option<LoadHandle>;

    /// Returns the AssetUUID for the given LoadHandle, if present.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    fn get_asset_id(&self, load: LoadHandle) -> Option<AssetUuid>;
}
