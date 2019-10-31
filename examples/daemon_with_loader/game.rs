use crate::image::Image;
use atelier_loader::{
    asset_uuid, rpc_loader::RpcLoader, AssetLoadOp, AssetStorage, AssetTypeId, AssetUuid,
    LoadHandle, LoadStatus, Loader, LoaderInfoProvider, TypeUuid,
};
use std::{cell::RefCell, collections::HashMap, error::Error};

struct AssetState<A> {
    version: u32,
    asset: A,
}
pub struct Storage<A> {
    assets: RefCell<HashMap<LoadHandle, AssetState<A>>>,
    uncommitted: RefCell<HashMap<LoadHandle, AssetState<A>>>,
}
impl<A> Storage<A> {
    fn new() -> Self {
        Self {
            assets: RefCell::new(HashMap::new()),
            uncommitted: RefCell::new(HashMap::new()),
        }
    }
}
// Implementation of AssetStorage for the typed storage
impl<A: for<'a> serde::Deserialize<'a>> AssetStorage for Storage<A> {
    fn update_asset(
        &self,
        loader_info: &dyn LoaderInfoProvider,
        asset_type_id: &AssetTypeId,
        data: &[u8],
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error>> {
        let mut uncommitted = self.uncommitted.borrow_mut();
        uncommitted.insert(
            load_handle,
            AssetState {
                asset: bincode::deserialize::<A>(data)?,
                version,
            },
        );
        log::info!("{} bytes loaded for {:?}", data.len(), load_handle);
        // The loading process could be async, in which case you can delay
        // calling `load_op.complete` as it should only be done when the asset is usable.
        load_op.complete();
        Ok(())
    }
    fn commit_asset_version(
        &self,
        asset_type: &AssetTypeId,
        load_handle: LoadHandle,
        version: u32,
    ) {
        // The commit step is done after an asset load has completed.
        // It exists to avoid frames where an asset that was loaded is unloaded, which
        // could happen when hot reloading. To support this case, you must support having multiple
        // versions of an asset loaded at the same time.
        let mut committed = self.assets.borrow_mut();
        let mut uncommitted = self.uncommitted.borrow_mut();
        committed.insert(
            load_handle,
            uncommitted
                .remove(&load_handle)
                .expect("asset not present when committing"),
        );
        log::info!("Commit {:?}", load_handle);
    }
    fn free(&self, asset_type_id: &AssetTypeId, load_handle: LoadHandle) {
        let mut committed = self.assets.borrow_mut();
        committed.remove(&load_handle);
        log::info!("Free {:?}", load_handle);
    }
}
struct Game {
    storage: HashMap<AssetTypeId, Box<dyn AssetStorage>>,
}

// Untyped implementation of AssetStorage that finds the asset_type's storage and forwards the call
impl AssetStorage for Game {
    fn update_asset(
        &self,
        loader_info: &dyn LoaderInfoProvider,
        asset_type_id: &AssetTypeId,
        data: &[u8],
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error>> {
        self.storage
            .get(asset_type_id)
            .expect("unknown asset type")
            .update_asset(
                loader_info,
                asset_type_id,
                data,
                load_handle,
                load_op,
                version,
            )
    }
    fn commit_asset_version(
        &self,
        asset_type: &AssetTypeId,
        load_handle: LoadHandle,
        version: u32,
    ) {
        self.storage
            .get(asset_type)
            .expect("unknown asset type")
            .commit_asset_version(asset_type, load_handle, version)
    }
    fn free(&self, asset_type_id: &AssetTypeId, load_handle: LoadHandle) {
        self.storage
            .get(asset_type_id)
            .expect("unknown asset type")
            .free(asset_type_id, load_handle)
    }
}

pub fn run() {
    let mut game = Game {
        storage: HashMap::new(),
    };
    // Create storage for Image type
    game.storage
        .insert(AssetTypeId(Image::UUID), Box::new(Storage::<Image>::new()));

    let mut loader = RpcLoader::default();
    let handle = loader.add_ref(asset_uuid!("6c5ae1ad-ae30-471b-985b-7d017265f19f"));
    loop {
        loader.process(&game).expect("failed to process loader");
        if let LoadStatus::Loaded = loader.get_load_status(handle) {
            break;
        }
    }
    // The basic API uses explicit reference counting.
    // Integrate with atelier_loader::handle for automatic reference counting!
    loader.remove_ref(handle);
    loop {
        loader.process(&game).expect("failed to process loader");
        if let LoadStatus::NotRequested = loader.get_load_status(handle) {
            break;
        }
    }
}
