use crate::custom_asset::BigPerf;
use crate::image::Image;
use atelier_loader::{
    asset_uuid,
    crossbeam_channel::{Receiver, Sender},
    handle::{AssetHandle, Handle, RefOp, TypedAssetStorage, WeakHandle},
    rpc_loader::RpcLoader,
    AssetLoadOp, AssetStorage, AssetTypeId, AssetUuid, LoadHandle, LoadStatus, Loader,
    LoaderInfoProvider, TypeUuid,
};
use mopa::{mopafy, Any};
use std::{cell::RefCell, collections::HashMap, error::Error, sync::Arc};

struct AssetState<A> {
    version: u32,
    asset: A,
}
pub struct Storage<A: TypeUuid> {
    refop_sender: Arc<Sender<RefOp>>,
    assets: HashMap<LoadHandle, AssetState<A>>,
    uncommitted: HashMap<LoadHandle, AssetState<A>>,
}
impl<A: TypeUuid> Storage<A> {
    fn new(sender: Arc<Sender<RefOp>>) -> Self {
        Self {
            refop_sender: sender,
            assets: HashMap::new(),
            uncommitted: HashMap::new(),
        }
    }
    fn get<T: AssetHandle>(&self, handle: &T) -> Option<&A> {
        self.assets.get(&handle.load_handle()).map(|a| &a.asset)
    }
    fn get_version<T: AssetHandle>(&self, handle: &T) -> Option<u32> {
        self.assets.get(&handle.load_handle()).map(|a| a.version)
    }
    fn get_asset_with_version<T: AssetHandle>(&self, handle: &T) -> Option<(&A, u32)> {
        self.assets
            .get(&handle.load_handle())
            .map(|a| (&a.asset, a.version))
    }
}
impl<A: TypeUuid + for<'a> serde::Deserialize<'a> + 'static> TypedAssetStorage<A> for Game {
    fn get<T: AssetHandle>(&self, handle: &T) -> Option<&A> {
        unsafe {
            std::mem::transmute(
                self.storage
                    .borrow()
                    .get(&AssetTypeId(A::UUID))
                    .expect("unknown asset type")
                    .as_ref()
                    .downcast_ref::<Storage<A>>()
                    .expect("failed to downcast")
                    .get(handle),
            )
        }
    }
    fn get_version<T: AssetHandle>(&self, handle: &T) -> Option<u32> {
        self.storage
            .borrow()
            .get(&AssetTypeId(A::UUID))
            .expect("unknown asset type")
            .as_ref()
            .downcast_ref::<Storage<A>>()
            .expect("failed to downcast")
            .get_version(handle)
    }
    fn get_asset_with_version<T: AssetHandle>(&self, handle: &T) -> Option<(&A, u32)> {
        unsafe {
            std::mem::transmute(
                self.storage
                    .borrow()
                    .get(&AssetTypeId(A::UUID))
                    .expect("unknown asset type")
                    .as_ref()
                    .downcast_ref::<Storage<A>>()
                    .expect("failed to downcast")
                    .get_asset_with_version(handle),
            )
        }
    }
}
pub trait TypedStorage: Any {
    fn update_asset(
        &mut self,
        loader_info: &dyn LoaderInfoProvider,
        data: &[u8],
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error>>;
    fn commit_asset_version(&mut self, handle: LoadHandle, version: u32);
    fn free(&mut self, handle: LoadHandle);
}
mopafy!(TypedStorage);
impl<A: for<'a> serde::Deserialize<'a> + 'static + TypeUuid> TypedStorage for Storage<A> {
    fn update_asset(
        &mut self,
        loader_info: &dyn LoaderInfoProvider,
        data: &[u8],
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error>> {
        // To enable automatic serde of Handle, we need to set up a SerdeContext with a RefOp sender
        let asset = atelier_loader::handle::SerdeContext::with(
            loader_info,
            self.refop_sender.clone(),
            || bincode::deserialize::<A>(data),
        )?;
        self.uncommitted
            .insert(load_handle, AssetState { asset, version });
        log::info!("{} bytes loaded for {:?}", data.len(), load_handle);
        // The loading process could be async, in which case you can delay
        // calling `load_op.complete` as it should only be done when the asset is usable.
        load_op.complete();
        Ok(())
    }
    fn commit_asset_version(&mut self, load_handle: LoadHandle, version: u32) {
        // The commit step is done after an asset load has completed.
        // It exists to avoid frames where an asset that was loaded is unloaded, which
        // could happen when hot reloading. To support this case, you must support having multiple
        // versions of an asset loaded at the same time.
        self.assets.insert(
            load_handle,
            self.uncommitted
                .remove(&load_handle)
                .expect("asset not present when committing"),
        );
        log::info!("Commit {:?}", load_handle);
    }
    fn free(&mut self, load_handle: LoadHandle) {
        self.assets.remove(&load_handle);
        log::info!("Free {:?}", load_handle);
    }
}
struct Game {
    storage: RefCell<HashMap<AssetTypeId, Box<dyn TypedStorage>>>,
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
            .borrow_mut()
            .get_mut(asset_type_id)
            .expect("unknown asset type")
            .update_asset(loader_info, data, load_handle, load_op, version)
    }
    fn commit_asset_version(
        &self,
        asset_type: &AssetTypeId,
        load_handle: LoadHandle,
        version: u32,
    ) {
        self.storage
            .borrow_mut()
            .get_mut(asset_type)
            .expect("unknown asset type")
            .commit_asset_version(load_handle, version)
    }
    fn free(&self, asset_type_id: &AssetTypeId, load_handle: LoadHandle) {
        self.storage
            .borrow_mut()
            .get_mut(asset_type_id)
            .expect("unknown asset type")
            .free(load_handle)
    }
}

fn process(loader: &mut RpcLoader, game: &Game, chan: &Receiver<RefOp>) {
    loop {
        match chan.try_recv() {
            Err(_) => break,
            Ok(RefOp::Decrease(handle)) => loader.remove_ref(handle),
            Ok(RefOp::Increase(handle)) => {
                loader
                    .get_load_info(handle)
                    .map(|info| loader.add_ref(info.asset_id));
            }
            Ok(RefOp::IncreaseUuid(uuid)) => {
                loader.add_ref(uuid);
            }
        }
    }
    loader.process(game).expect("failed to process loader");
}
fn init_storages(game: &mut Game, tx: Arc<Sender<RefOp>>) {
    let mut storages = game.storage.borrow_mut();
    // Create storage for types
    storages.insert(
        AssetTypeId(Image::UUID),
        Box::new(Storage::<Image>::new(tx.clone())),
    );
    storages.insert(
        AssetTypeId(BigPerf::UUID),
        Box::new(Storage::<BigPerf>::new(tx.clone())),
    );
}
pub fn run() {
    let mut game = Game {
        storage: RefCell::new(HashMap::new()),
    };
    let (tx, rx) = atelier_loader::crossbeam_channel::unbounded();
    let tx = Arc::new(tx);
    init_storages(&mut game, tx.clone());

    let mut loader = RpcLoader::default();
    let handle = loader.add_ref(asset_uuid!("6f91796a-6a14-4dd2-8b7b-cb5369417032"));
    loop {
        process(&mut loader, &game, &rx);
        if let LoadStatus::Loaded = loader.get_load_status(handle) {
            break;
        }
    }
    let custom_asset: &BigPerf = WeakHandle::new(handle)
        .asset(&game)
        .expect("failed to get asset");
    log::info!(
        "Image has handle {:?}",
        custom_asset.but_cooler_handle.clone()
    );
    // The basic API uses explicit reference counting.
    // atelier_loader::handle module provides automatic reference counting
    loader.remove_ref(handle);
    loop {
        process(&mut loader, &game, &rx);
        if let LoadStatus::NotRequested = loader.get_load_status(handle) {
            break;
        }
    }
}
