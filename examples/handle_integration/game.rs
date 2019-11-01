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
        let handle = loader.add_ref(asset_uuid!("6f91796a-6a14-4dd2-8b7b-cb5369417032"));
        let handle = Handle::<BigPerf>::new(tx.clone(), handle);
        loop {
            process(&mut loader, &game, &rx);
            if let LoadStatus::Loaded = handle.load_status(&loader) {
                break;
            }
        }
        let custom_asset: &BigPerf = handle.asset(&game.storage).expect("failed to get asset");
        log::info!(
            "Image dependency has handle {:?}",
            custom_asset.but_cooler_handle.load_handle()
        );
        // Handle is automatically refcounted, so it will be dropped at the end of this scope,
        // causing all assets to be unloaded. We return a WeakHandle of the image dependency
        //  to be able to track this unload
        WeakHandle::new(custom_asset.but_cooler_handle.load_handle())
    };
    loop {
        process(&mut loader, &game, &rx);
        if let LoadStatus::NotRequested = weak_handle.load_status(&loader) {
            break;
        }
    }
}
