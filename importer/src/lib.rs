mod amethyst_formats;
mod boxed_importer;
mod error;
mod serde_obj;

use serde::{Deserialize, Serialize};
use serde_dyn::{TypeUuid, uuid};
use std::io::Read;

pub use self::error::{Error, Result};
// pub use crate::amethyst_formats::amethyst_formats;
pub use crate::boxed_importer::{
    AssetMetadata, BoxedImporter, SourceMetadata, SOURCEMETADATA_VERSION, SourceFileImporter, get_source_importers,
};
pub use inventory;
pub use crate::serde_obj::SerdeObj;

pub type AssetUUID = ::uuid::Uuid;
pub type AssetTypeId = [u8; 16];

/// A format, providing a conversion from bytes to asset data, which is then
/// in turn accepted by `Asset::from_data`. Examples for formats are
/// `Png`, `Obj` and `Wave`.
pub trait Importer: Send + 'static {
    /// Version of the importer serialization format.
    fn version_static() -> u32
    where
        Self: Sized;
    /// Version of the importer serialization format.
    fn version(&self) -> u32;
    /// Options specific to the format, which are passed to `import`.
    /// E.g. for textures this would be stuff like mipmap levels and
    /// sampler info.
    type Options: Send + 'static;

    /// State is specific to the format, which are passed to `import`.
    /// This is maintained by the asset pipeline to enable Importers to
    /// store state between calls to import().
    /// This is primarily used to store generated AssetUUIDs with mappings to
    /// format-internal identifiers and ensure IDs are stable between imports.
    type State: Serialize + Send + 'static;

    /// Reads the given bytes and produces asset data.
    fn import(
        &self,
        source: &mut dyn Read,
        options: Self::Options,
        state: &mut Self::State,
    ) -> Result<ImporterValue>;
}

/// Contains metadata and asset data for an imported asset
pub struct ImportedAsset {
    /// UUID for the asset to uniquely identify it
    pub id: AssetUUID,
    /// Search tags that are used by asset tooling to search for the imported asset
    pub search_tags: Vec<(String, Option<String>)>,
    /// Build dependencies will be included in the Builder arguments when building the asset
    pub build_deps: Vec<AssetUUID>,
    /// Load dependencies will be loaded before this asset in the Loader
    pub load_deps: Vec<AssetUUID>,
    /// Instantiate dependencies will be instantiated along with this asset when
    /// the asset is instantiated into a world
    pub instantiate_deps: Vec<AssetUUID>,
    /// The referenced build pipeline is invoked when a build artifact is requested for the imported asset
    pub build_pipeline: Option<AssetUUID>,
    /// The actual asset data used by tools and Builder
    pub asset_data: Box<dyn SerdeObj>,
}

/// Return value for Importers containing all imported assets
pub struct ImporterValue {
    /// All imported assets
    pub assets: Vec<ImportedAsset>,
}
