#![deny(
    rust_2018_compatibility,
    rust_2018_idioms,
    unused,
    unused_extern_crates,
    future_incompatible,
    nonstandard_style
)]

mod boxed_importer;
mod error;
mod serde_obj;
mod serialized_asset;

#[cfg(feature = "serde_importers")]
mod ron_importer;
use std::io::{Read, Write};
use std::path::Path;

pub use distill_core::{
    importer_context::{ImporterContext, ImporterContextHandle},
    ArtifactMetadata, AssetMetadata,
};
use distill_core::{AssetRef, AssetUuid};
#[cfg(feature = "serde_importers")]
pub use distill_serde_importable_derive::*;
use futures::{future::BoxFuture, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub use serde;
use serde::Serialize;

pub use self::error::{Error, Result};
#[cfg(feature = "serde_importers")]
pub use crate::ron_importer::{RonImporter, RonImporterOptions, RonImporterState};
#[doc(hidden)]
#[cfg(feature = "serde_importers")]
pub use crate::serde_obj::typetag;
#[cfg(feature = "serde_importers")]
pub use crate::serde_obj::SerdeImportable;
pub use crate::{
    boxed_importer::{BoxedImporter, SourceMetadata, SOURCEMETADATA_VERSION},
    serde_obj::{IntoSerdeObj, SerdeObj},
    serialized_asset::SerializedAsset,
};

#[derive(Default)]
pub struct ImportOp {
    pub errors: Vec<Box<dyn std::error::Error + Send + 'static>>,
    pub warnings: Vec<Box<dyn std::error::Error + Send + 'static>>,
}

impl ImportOp {
    pub fn error<T: Into<Box<dyn std::error::Error + Send + 'static>>>(&mut self, err: T) {
        self.errors.push(err.into());
    }

    pub fn warn<T: Into<Box<dyn std::error::Error + Send + 'static>>>(&mut self, err: T) {
        self.warnings.push(err.into());
    }

    pub fn new_asset_uuid(&self) -> AssetUuid {
        AssetUuid(*uuid::Uuid::new_v4().as_bytes())
    }
}

pub enum ImportSource<'a> {
    File(&'a Path),
}

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
    type Options: Send + Sync + 'static;

    /// State is maintained by the asset pipeline to enable Importers to
    /// store state between calls to import().
    /// This is primarily used to ensure IDs are stable between imports
    /// by storing generated AssetUuids with mappings to format-internal identifiers.
    type State: Serialize + Send + 'static;

    fn default_options(&self, _import_source: ImportSource<'_>) -> Option<Self::Options> {
        None
    }

    /// Reads the given bytes and produces assets.
    fn import(
        &self,
        op: &mut ImportOp,
        source: &mut dyn Read,
        options: &Self::Options,
        state: &mut Self::State,
    ) -> Result<ImporterValue>;

    /// Writes a set of assets to a source file format that can be read by `import`.
    fn export(
        &self,
        _output: &mut dyn Write,
        _options: &Self::Options,
        _state: &mut Self::State,
        _assets: Vec<ExportAsset>,
    ) -> Result<ImporterValue> {
        Err(Error::ExportUnsupported)
    }
}

/// Importers parse file formats and produce assets.
pub trait AsyncImporter: Send + 'static {
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
    type Options: Send + Sync + 'static;

    /// State is maintained by the asset pipeline to enable Importers to
    /// store state between calls to import().
    /// This is primarily used to ensure IDs are stable between imports
    /// by storing generated AssetUuids with mappings to format-internal identifiers.
    type State: Serialize + Send + 'static;

    fn default_options<'a>(&'a self, _import_source: ImportSource<'_>) -> Option<Self::Options>;

    /// Reads the given bytes and produces assets.
    fn import<'a>(
        &'a self,
        op: &'a mut ImportOp,
        source: &'a mut (dyn AsyncRead + Unpin + Send + Sync),
        options: &'a Self::Options,
        state: &'a mut Self::State,
    ) -> BoxFuture<'a, Result<ImporterValue>>;

    /// Writes a set of assets to a source file format that can be read by `import`.
    fn export<'a>(
        &'a self,
        _output: &'a mut (dyn AsyncWrite + Unpin + Send + Sync),
        _options: &'a Self::Options,
        _state: &'a mut Self::State,
        _assets: Vec<ExportAsset>,
    ) -> BoxFuture<'a, Result<ImporterValue>> {
        Box::pin(async move { Err(Error::ExportUnsupported) })
    }
}

impl<T: Importer + Sync> AsyncImporter for T {
    /// Options can store settings that change importer behaviour.
    /// Will be automatically stored in .meta files and passed to [Importer::import].
    type Options = <T as Importer>::Options;
    /// State is maintained by the asset pipeline to enable Importers to
    /// store state between calls to import().
    /// This is primarily used to ensure IDs are stable between imports
    /// by storing generated AssetUuids with mappings to format-internal identifiers.
    type State = <T as Importer>::State;

    fn version_static() -> u32
    where
        Self: Sized,
    {
        <T as Importer>::version_static()
    }

    fn version(&self) -> u32 {
        <T as Importer>::version(self)
    }

    fn default_options<'a>(&'a self, import_source: ImportSource<'_>) -> Option<Self::Options> {
        <T as Importer>::default_options(self, import_source)
    }

    /// Reads the given bytes and produces assets.
    fn import<'a>(
        &'a self,
        op: &'a mut ImportOp,
        source: &'a mut (dyn AsyncRead + Unpin + Send + Sync),
        options: &'a Self::Options,
        state: &'a mut Self::State,
    ) -> BoxFuture<'a, Result<ImporterValue>> {
        Box::pin(async move {
            let mut bytes = Vec::new();
            source.read_to_end(&mut bytes).await?;
            let mut reader = bytes.as_slice();
            <T as Importer>::import(self, op, &mut reader, options, state)
        })
    }

    /// Writes a set of assets to a source file format that can be read by `import`.
    fn export<'a>(
        &'a self,
        output: &'a mut (dyn AsyncWrite + Unpin + Send + Sync),
        options: &'a Self::Options,
        state: &'a mut Self::State,
        assets: Vec<ExportAsset>,
    ) -> BoxFuture<'a, Result<ImporterValue>> {
        Box::pin(async move {
            let mut write_buf = Vec::new();
            let result = <T as Importer>::export(self, &mut write_buf, options, state, assets)?;
            output.write(&write_buf).await?;
            Ok(result)
        })
    }
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

/// Input to Importer::export
pub struct ExportAsset {
    /// Asset to be exported
    pub asset: SerializedAsset<Vec<u8>>,
}

#[cfg(feature = "serde_importers")]
#[macro_export]
macro_rules! if_serde_importers {
    ($($tt:tt)*) => {
        $($tt)*
    }
}

#[cfg(not(feature = "serde_importers"))]
#[macro_export]
#[doc(hidden)]
macro_rules! if_serde_importers {
    ($($tt:tt)*) => {};
}

/// Convenience function for reporting an error in an `Importer`
pub fn import_error<T: Into<String>>(text: String) -> Box<dyn std::error::Error + Send + 'static> {
    Box::new(Error::Custom(text))
}
