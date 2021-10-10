use std::{collections::HashMap, error::Error};

use distill::{
    core::type_uuid::TypeUuid,
    loader::{
        loader::Loader,
        storage::{
            AssetLoadOp, AssetStorage, DefaultIndirectionResolver, IndirectionTable, LoadHandle,
            LoadStatus, LoaderInfoProvider,
        },
        AssetTypeId, RpcIO,
    },
};

use crate::image::Image;

#[allow(dead_code)]
struct AssetState<A> {
    version: u32,
    asset: A,
}
pub struct Storage<A> {
    assets: HashMap<LoadHandle, AssetState<A>>,
    uncommitted: HashMap<LoadHandle, AssetState<A>>,
    indirection_table: IndirectionTable,
}
impl<A> Storage<A> {
    fn new(indirection_table: IndirectionTable) -> Self {
        Self {
            assets: HashMap::new(),
            uncommitted: HashMap::new(),
            indirection_table,
        }
    }

    pub fn get_asset(&self, handle: LoadHandle) -> Option<&A> {
        let handle = if handle.is_indirect() {
            self.indirection_table.resolve(handle)?
        } else {
            handle
        };
        let asset = self.assets.get(&handle);
        asset.map(|state| &state.asset)
    }
}
// Implementation of AssetStorage for the typed storage
impl<A: for<'a> serde::Deserialize<'a>> AssetStorage for Storage<A> {
    fn update_asset(
        &mut self,
        _loader_info: &dyn LoaderInfoProvider,
        _asset_type_id: &AssetTypeId,
        data: Vec<u8>,
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error + Send + 'static>> {
        self.uncommitted.insert(
            load_handle,
            AssetState {
                asset: bincode::deserialize::<A>(&data).expect("failed to deserialize asset"),
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
        &mut self,
        _asset_type: &AssetTypeId,
        load_handle: LoadHandle,
        _version: u32,
    ) {
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

    fn free(&mut self, _asset_type_id: &AssetTypeId, load_handle: LoadHandle, version: u32) {
        if let Some(asset) = self.uncommitted.get(&load_handle) {
            if asset.version == version {
                self.uncommitted.remove(&load_handle);
            }
        }
        if let Some(asset) = self.assets.get(&load_handle) {
            if asset.version == version {
                self.assets.remove(&load_handle);
            }
        }
        log::info!("Free {:?}", load_handle);
    }
}
struct Game {
    storage: HashMap<AssetTypeId, Box<dyn AssetStorage>>,
}

// Untyped implementation of AssetStorage that finds the asset_type's storage and forwards the call
impl AssetStorage for Game {
    fn update_asset(
        &mut self,
        loader_info: &dyn LoaderInfoProvider,
        asset_type_id: &AssetTypeId,
        data: Vec<u8>,
        load_handle: LoadHandle,
        load_op: AssetLoadOp,
        version: u32,
    ) -> Result<(), Box<dyn Error + Send + 'static>> {
        self.storage
            .get_mut(asset_type_id)
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
        &mut self,
        asset_type: &AssetTypeId,
        load_handle: LoadHandle,
        version: u32,
    ) {
        self.storage
            .get_mut(asset_type)
            .expect("unknown asset type")
            .commit_asset_version(asset_type, load_handle, version)
    }

    fn free(&mut self, asset_type_id: &AssetTypeId, load_handle: LoadHandle, version: u32) {
        self.storage
            .get_mut(asset_type_id)
            .expect("unknown asset type")
            .free(asset_type_id, load_handle, version)
    }
}

pub fn run() {
    let mut game = Game {
        storage: HashMap::new(),
    };
    let mut loader = Loader::new(Box::new(RpcIO::default()));
    // Create storage for Image type
    game.storage.insert(
        AssetTypeId(Image::UUID),
        Box::new(Storage::<Image>::new(loader.indirection_table())),
    );

    let handle = loader.add_ref("6c5ae1ad-ae30-471b-985b-7d017265f19f");
    loop {
        loader
            .process(&mut game, &DefaultIndirectionResolver)
            .expect("failed to process loader");
        if let LoadStatus::Loaded = loader.get_load_status(handle) {
            break;
        }
    }
    // The basic API uses explicit reference counting.
    // Integrate with distill_loader::handle for automatic reference counting!
    loader.remove_ref(handle);
    loop {
        loader
            .process(&mut game, &DefaultIndirectionResolver)
            .expect("failed to process loader");
        if let LoadStatus::NotRequested = loader.get_load_status(handle) {
            break;
        }
    }
}
