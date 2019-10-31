mod boxed_importer;
mod error;
mod serde_obj;

#[cfg(feature = "serde_importers")]
mod ron_importer;
#[cfg(feature = "serde_importers")]
pub use crate::ron_importer::{RonImporter, RonImporterOptions, RonImporterState};
#[doc(hidden)]
#[cfg(feature = "serde_importers")]
pub use crate::serde_obj::typetag;

use atelier_core::{AssetRef, AssetUuid};
use serde::Serialize;
use std::io::Read;

pub use self::error::{Error, Result};
pub use crate::{
    boxed_importer::{
        get_source_importers, AssetMetadata, BoxedImporter, SourceFileImporter, SourceMetadata,
        SOURCEMETADATA_VERSION,
    },
    serde_obj::{IntoSerdeObj, SerdeImportable, SerdeObj},
};
pub use atelier_core::importer_context::{
    get_importer_contexts, ImporterContext, ImporterContextHandle, ImporterContextRegistration,
};
pub use inventory;

/// Importers parse file formats and produce assets.
pub trait Importer: Send + 'static {
    /// Returns the version of the importer.
    /// This version should change any time the importer behaviour changes to
    /// trigger reimport of assets.
    fn version_static() -> u32
    where
        Self: Sized;
    /// Returns the version of the importer.
    /// This version should change any time the importer behaviour changes to
    /// trigger reimport of assets.
    fn version(&self) -> u32;

    /// Options can store settings that change importer behaviour.
    /// Will be automatically stored in .meta files and passed to [Importer::import].
    type Options: Send + 'static;

    /// State is maintained by the asset pipeline to enable Importers to
    /// store state between calls to import().
    /// This is primarily used to ensure IDs are stable between imports
    /// by storing generated AssetUuids with mappings to format-internal identifiers.
    type State: Serialize + Send + 'static;

    /// Reads the given bytes and produces assets.
    fn import(
        &self,
        source: &mut dyn Read,
        options: Self::Options,
        state: &mut Self::State,
    ) -> Result<ImporterValue>;
}

/// Contains metadata and asset data for an imported asset.
/// Produced by [Importer] implementations.
pub struct ImportedAsset {
    /// UUID for the asset to uniquely identify it.
    pub id: AssetUuid,
    /// Search tags are used by asset tooling to search for the imported asset.
    pub search_tags: Vec<(String, Option<String>)>,
    /// Build dependencies will be included in the Builder arguments when building the asset.
    pub build_deps: Vec<AssetRef>,
    /// Load dependencies are guaranteed to load before this asset.
    pub load_deps: Vec<AssetRef>,
    /// The referenced build pipeline is invoked when a build artifact is requested for the imported asset.
    pub build_pipeline: Option<AssetUuid>,
    /// The actual asset data used by tools and Builder.
    pub asset_data: Box<dyn SerdeObj>,
}

/// Return value for Importers containing all imported assets.
pub struct ImporterValue {
    pub assets: Vec<ImportedAsset>,
}
