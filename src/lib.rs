pub use atelier_core as core;
pub use atelier_daemon as daemon;
pub use atelier_importer as importer;
pub use atelier_loader as loader;

use atelier_core::{AssetRef, AssetUuid};
use atelier_loader::handle::{Handle, SerdeContext};

pub fn make_handle<T>(uuid: AssetUuid) -> Handle<T> {
    SerdeContext::with_active(|loader_info_provider, ref_op_sender| {
        let load_handle = loader_info_provider
            .get_load_handle(&AssetRef::Uuid(uuid))
            .unwrap();
        Handle::<T>::new(ref_op_sender.clone(), load_handle)
    })
}

pub fn make_handle_from_str<T>(uuid_str: &str) -> Result<Handle<T>, atelier_core::uuid::Error> {
    use std::str::FromStr;
    Ok(make_handle(AssetUuid(
        *atelier_core::uuid::Uuid::from_str(uuid_str)?.as_bytes(),
    )))
}
