use crate::{AssetRef, AssetUuid};
use futures::future::BoxFuture;

pub trait ImporterContextHandle: Send + Sync {
    fn scope<'a>(&'a self, fut: BoxFuture<'a, ()>) -> BoxFuture<'a, ()>;

    fn begin_serialize_asset(&mut self, asset: AssetUuid);
    /// Returns any registered dependencies
    fn end_serialize_asset(&mut self, asset: AssetUuid) -> std::collections::HashSet<AssetRef>;
    /// Resolves an AssetRef to a specific AssetUuid
    fn resolve_ref(&mut self, asset_ref: &AssetRef, asset: AssetUuid);
}

pub trait ImporterContext: 'static + Send + Sync {
    fn handle(&self) -> Box<dyn ImporterContextHandle>;
}
