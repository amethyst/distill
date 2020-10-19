use crate::{error::Result, AsyncImporter, ExportAsset, ImporterValue, SerdeObj};
use atelier_core::{AssetMetadata, AssetTypeId, TypeUuidDynamic};
use erased_serde::Deserializer;
use futures_core::future::BoxFuture;
use futures_io::{AsyncRead, AsyncWrite};
use serde::{Deserialize, Serialize};

/// Version of the SourceMetadata struct.
/// Used for forward compatibility to enable changing the .meta file format
pub const SOURCEMETADATA_VERSION: u32 = 1;

/// SourceMetadata is the in-memory representation of the .meta file for a (source, .meta) pair.
#[derive(Serialize, Deserialize)]
pub struct SourceMetadata<Options: 'static, State: 'static> {
    /// Metadata struct version
    pub version: u32,
    /// Hash of the source file + importer options + importer state when last importing source file.
    pub import_hash: Option<u64>,
    /// The [`crate::Importer::version`] used to import the source file.
    pub importer_version: u32,
    /// The [`TypeUuidDynamic::uuid`] used to import the source file.
    #[serde(default)]
    pub importer_type: AssetTypeId,
    /// The [`crate::Importer::Options`] used to import the source file.
    pub importer_options: Options,
    /// The [`crate::Importer::State`] generated when importing the source file.
    pub importer_state: State,
    /// Metadata for assets generated when importing the source file.
    pub assets: Vec<AssetMetadata>,
}

/// Trait object wrapper for [`crate::Importer`] implementations.
/// Enables using Importers without knowing the concrete type.
/// See [`crate::Importer`] for documentation on fields.
pub trait BoxedImporter: TypeUuidDynamic + Send + Sync + 'static {
    fn import_boxed<'a>(
        &'a self,
        source: &'a mut (dyn AsyncRead + Unpin + Send + Sync),
        options: Box<dyn SerdeObj>,
        state: Box<dyn SerdeObj>,
    ) -> BoxFuture<'a, Result<BoxedImporterValue>>;
    fn export_boxed<'a>(
        &'a self,
        output: &'a mut (dyn AsyncWrite + Unpin + Send + Sync),
        options: Box<dyn SerdeObj>,
        state: Box<dyn SerdeObj>,
        assets: Vec<ExportAsset>,
    ) -> BoxFuture<'a, Result<BoxedExportInputs>>;
    fn default_options(&self) -> Box<dyn SerdeObj>;
    fn default_state(&self) -> Box<dyn SerdeObj>;
    fn version(&self) -> u32;
    fn deserialize_metadata(
        &self,
        deserializer: &mut dyn Deserializer,
    ) -> Result<SourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>>;
    fn deserialize_options(&self, deserializer: &mut dyn Deserializer)
        -> Result<Box<dyn SerdeObj>>;
    fn deserialize_state(&self, deserializer: &mut dyn Deserializer) -> Result<Box<dyn SerdeObj>>;
}

impl std::fmt::Debug for dyn BoxedImporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoxedImporter").field(&self.uuid()).finish()
    }
}

/// Trait object wrapper for [ImporterValue] implementations.
/// See [ImporterValue] for documentation on fields.
pub struct BoxedImporterValue {
    pub value: ImporterValue,
    pub options: Box<dyn SerdeObj>,
    pub state: Box<dyn SerdeObj>,
}

/// Return value for BoxedImporter::export_boxed
pub struct BoxedExportInputs {
    pub options: Box<dyn SerdeObj>,
    pub state: Box<dyn SerdeObj>,
    pub value: ImporterValue,
}

impl<S, O, T> BoxedImporter for T
where
    O: SerdeObj + Serialize + Default + Send + Sync + Clone + for<'a> Deserialize<'a>,
    S: SerdeObj + Serialize + Default + Send + Sync + for<'a> Deserialize<'a>,
    T: AsyncImporter<State = S, Options = O> + TypeUuidDynamic + Send + Sync,
{
    fn import_boxed<'a>(
        &'a self,
        source: &'a mut (dyn AsyncRead + Unpin + Send + Sync),
        options: Box<dyn SerdeObj>,
        mut state: Box<dyn SerdeObj>,
    ) -> BoxFuture<'a, Result<BoxedImporterValue>> {
        log::trace!("import_boxed");
        Box::pin(async move {
            let s = state.any_mut().downcast_mut::<S>();
            let s = if let Some(s) = s {
                s
            } else {
                panic!("Failed to downcast Importer::State");
            };
            let o = options.any().downcast_ref::<O>();
            let o = if let Some(o) = o {
                o
            } else {
                panic!("Failed to downcast Importer::Options");
            };

            log::trace!("import_boxed about to import");
            let result = self.import(source, o, s).await?;
            log::trace!("import_boxed imported");
            Ok(BoxedImporterValue {
                value: result,
                options,
                state,
            })
        })
    }

    fn export_boxed<'a>(
        &'a self,
        output: &'a mut (dyn AsyncWrite + Unpin + Send + Sync),
        options: Box<dyn SerdeObj>,
        mut state: Box<dyn SerdeObj>,
        assets: Vec<ExportAsset>,
    ) -> BoxFuture<'a, Result<BoxedExportInputs>> {
        Box::pin(async move {
            let s = state.any_mut().downcast_mut::<S>();
            let s = if let Some(s) = s {
                s
            } else {
                panic!("Failed to downcast Importer::State");
            };
            let o = options.any().downcast_ref::<O>();
            let o = if let Some(o) = o {
                o
            } else {
                panic!("Failed to downcast Importer::Options");
            };

            let result = self.export(output, o, s, assets).await?;
            Ok(BoxedExportInputs {
                options,
                state,
                value: result,
            })
        })
    }

    fn default_options(&self) -> Box<dyn SerdeObj> {
        Box::new(O::default())
    }

    fn default_state(&self) -> Box<dyn SerdeObj> {
        Box::new(S::default())
    }

    fn version(&self) -> u32 {
        T::version(self)
    }

    fn deserialize_metadata<'a>(
        &self,
        deserializer: &mut dyn Deserializer,
    ) -> Result<SourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>> {
        let metadata = erased_serde::deserialize::<SourceMetadata<O, S>>(deserializer)?;
        Ok(SourceMetadata {
            version: metadata.version,
            import_hash: metadata.import_hash,
            importer_version: metadata.importer_version,
            importer_type: metadata.importer_type,
            importer_options: Box::new(metadata.importer_options),
            importer_state: Box::new(metadata.importer_state),
            assets: metadata.assets,
        })
    }

    fn deserialize_options<'a>(
        &self,
        deserializer: &mut dyn Deserializer,
    ) -> Result<Box<dyn SerdeObj>> {
        Ok(Box::new(erased_serde::deserialize::<O>(deserializer)?))
    }

    fn deserialize_state<'a>(
        &self,
        deserializer: &mut dyn Deserializer,
    ) -> Result<Box<dyn SerdeObj>> {
        Ok(Box::new(erased_serde::deserialize::<S>(deserializer)?))
    }
}
