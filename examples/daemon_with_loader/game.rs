use crate::image::Image;
use atelier_loader::{
    asset_uuid, rpc_loader::RpcLoader, AssetLoadOp, AssetRef, AssetStorage, AssetTypeId, AssetUuid,
    LoadHandle, LoadStatus, Loader, LoaderInfoProvider, TypeUuid,
};
use std::error::Error;
struct Game;

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
        log::info!("{} bytes loaded for {:?}", data.len(), load_handle);
        match asset_type_id.0 {
            Image::UUID => {
                let image = bincode::deserialize::<Image>(data)?;
            }
            _ => panic!("loaded unrecognized asset type"),
        }
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
        log::info!("Commit {:?}", load_handle);
    }
    fn free(&self, asset_type_id: &AssetTypeId, load_handle: LoadHandle) {
        log::info!("Free {:?}", load_handle);
    }
}

pub fn run() {
    let mut game = Game {};
    let mut loader = RpcLoader::default();
    let handle = loader.add_ref(asset_uuid!("6c5ae1ad-ae30-471b-985b-7d017265f19f"));
    loop {
        loader.process(&game).expect("failed to process loader");
        if let LoadStatus::Loaded = loader.get_load_status(handle) {
            break;
        }
    }
    loader.remove_ref(handle);
    loop {
        loader.process(&game).expect("failed to process loader");
        if let LoadStatus::NotRequested = loader.get_load_status(handle) {
            break;
        }
    }
}
