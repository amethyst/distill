mod amethyst_formats;
mod boxed_importer;
mod error;
mod serde_obj;

use amethyst::assets::{Asset, SimpleFormat};
use serde::{Deserialize, Serialize};
use serde_dyn::uuid;
use std::io::Read;

pub use self::error::{Error, Result};
pub use crate::amethyst_formats::amethyst_formats;
pub use crate::boxed_importer::{
    AssetMetadata, BoxedImporter, SourceMetadata, SOURCEMETADATA_VERSION,
};
pub use crate::serde_obj::SerdeObj;

pub type AssetUUID = ::uuid::Uuid;

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

// TODO: move SimpleImporter back to amethyst

/// A simple state for Importer to retain the same UUID between imports
/// for all single-asset source files
#[derive(Default, Serialize, Deserialize)]
pub struct SimpleImporterState {
    id: Option<AssetUUID>,
}
uuid! { SimpleImporterState => 276663539928909366810068622540168088635 }

/// Wrapper struct to be able to impl Importer for any SimpleFormat
pub struct SimpleImporter<A: Asset, T: SimpleFormat<A>>(pub T, ::std::marker::PhantomData<A>);

impl<A: Asset, T: SimpleFormat<A> + 'static> From<T> for SimpleImporter<A, T> {
    fn from(fmt: T) -> SimpleImporter<A, T> {
        SimpleImporter(fmt, ::std::marker::PhantomData)
    }
}

impl<A: Asset, T: SimpleFormat<A> + Send + 'static> Importer for SimpleImporter<A, T>
where
    <A as Asset>::Data: SerdeObj,
{
    type State = SimpleImporterState;
    type Options = T::Options;

    fn version_static() -> u32
    where
        Self: Sized,
    {
        1
    }
    fn version(&self) -> u32 {
        Self::version_static()
    }

    fn import(
        &self,
        source: &mut dyn Read,
        options: Self::Options,
        state: &mut Self::State,
    ) -> Result<ImporterValue> {
        if state.id.is_none() {
            state.id = Some(uuid::Uuid::new_v4());
        }
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes)?;
        let import_result = self.0.import(bytes, options)?;
        Ok(ImporterValue {
            assets: vec![ImportedAsset {
                id: state.id.expect("AssetUUID not generated"),
                search_tags: Vec::new(),
                build_deps: Vec::new(),
                load_deps: Vec::new(),
                instantiate_deps: Vec::new(),
                asset_data: Box::new(import_result),
                build_pipeline: None,
            }],
        })
    }
}
