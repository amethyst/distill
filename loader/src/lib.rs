#![warn(rust_2018_idioms, rust_2018_compatibility)]

#[cfg(feature = "handle")]
pub mod handle;
mod loader;
#[cfg(feature = "rpc_loader")]
mod rpc_state;
mod storage;

pub use crate::loader::{Loader, LoaderIO, LoaderState};
pub use crate::rpc_state::RpcIO;
pub use crate::storage::{
    AssetLoadOp, AssetStorage, AtomicHandleAllocator, HandleAllocator, LoadHandle, LoadInfo,
    LoadStatus, LoaderInfoProvider,
};
#[cfg(feature = "asset_uuid_macro")]
pub use atelier_core::asset_uuid;
pub use atelier_core::{AssetRef, AssetTypeId, AssetUuid};
pub use crossbeam_channel;
#[cfg(feature = "handle")]
pub use handle::HandleSerdeContextProvider;
pub use type_uuid::{TypeUuid, TypeUuidDynamic};

#[cfg(feature = "handle")]
#[macro_export]
macro_rules! if_handle_enabled {
    ($($tt:tt)*) => {
        $($tt)*
    };
}

#[cfg(not(feature = "handle"))]
#[macro_export]
#[doc(hidden)]
macro_rules! if_handle_enabled {
    ($($tt:tt)*) => {};
}
