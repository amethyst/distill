use crate::custom_asset::BigPerf;
use crate::image::Image;
use crate::storage::GenericAssetStorage;
use atelier_loader::{
    asset_uuid,
    crossbeam_channel::Receiver,
    handle::{AssetHandle, Handle, RefOp, WeakHandle},
    rpc_loader::RpcLoader,
    AssetUuid, LoadStatus, Loader,
};
use std::sync::Arc;

struct Game {
    storage: GenericAssetStorage,
}

fn process(loader: &mut RpcLoader, game: &Game, chan: &Receiver<RefOp>) {
    atelier_loader::handle::process_ref_ops(loader, chan);
    loader
        .process(&game.storage)
        .expect("failed to process loader");
}

pub fn run() {
    let (tx, rx) = atelier_loader::crossbeam_channel::unbounded();
    let tx = Arc::new(tx);
    let game = Game {
        storage: GenericAssetStorage::new(tx.clone()),
    };
    game.storage.add_storage::<Image>();
    game.storage.add_storage::<BigPerf>();

    let mut loader = RpcLoader::default();
    let weak_handle = {
        // add_ref begins loading of the asset
        let handle = loader.add_ref(asset_uuid!("6f91796a-6a14-4dd2-8b7b-cb5369417032"));
        // From the returned LoadHandle, create a typed, internally refcounted Handle.
        // This requires a channel to send increase/decrease over to be able to implement
        // Clone and Drop. In a real implementation, you would probably create nicer wrappers for this.
        let handle = Handle::<BigPerf>::new(tx.clone(), handle);
        loop {
            process(&mut loader, &game, &rx);
            if let LoadStatus::Loaded = handle.load_status(&loader) {
                break;
            }
        }
        // From the Storage, use the Handle to get a reference to the loaded asset.
        let custom_asset: &BigPerf = handle.asset(&game.storage).expect("failed to get asset");
        // The custom asset has an automatically constructed Handle reference to an Image.
        log::info!(
            "Image dependency has handle {:?} from path, and {:?} from UUID",
            custom_asset.handle_made_from_path.load_handle(),
            custom_asset.handle_made_from_uuid.load_handle()
        );
        // Handle is automatically refcounted, so it will be dropped at the end of this scope,
        // causing the asset and its dependencies to be unloaded.
        // We return a WeakHandle of the image dependency to be able to track the unload of the dependency,
        // which happens after the dependee.
        WeakHandle::new(custom_asset.handle_made_from_path.load_handle())
    };
    loop {
        process(&mut loader, &game, &rx);
        if let LoadStatus::NotRequested = weak_handle.load_status(&loader) {
            break;
        }
    }
}
