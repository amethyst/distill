use crate::error::Result;
use crate::{AssetTypeId, AssetUUID, Importer, ImporterValue, SerdeObj};
use ron;
use serde::{Deserialize, Serialize};
use std::io::Read;
use type_uuid::{TypeUuid, TypeUuidDynamic};

#[derive(Clone, Serialize, Deserialize, Hash, Default)]
pub struct AssetMetadata {
    pub id: AssetUUID,
    pub search_tags: Vec<(String, Option<String>)>,
    pub build_deps: Vec<AssetUUID>,
    pub load_deps: Vec<AssetUUID>,
    pub instantiate_deps: Vec<AssetUUID>,
    pub build_pipeline: Option<AssetUUID>,
    pub import_asset_type: AssetTypeId,
}
pub const SOURCEMETADATA_VERSION: u32 = 1;
#[derive(Serialize, Deserialize)]
pub struct SourceMetadata<Options, State> {
    /// Metadata struct version
    pub version: u32,
    /// Hash of the source file + importer options + importer state this metadata was generated from
    pub import_hash: Option<u64>,
    pub importer_version: u32,
    pub importer_options: Options,
    pub importer_state: State,
    pub assets: Vec<AssetMetadata>,
}

pub trait BoxedImporter: TypeUuidDynamic + Send + Sync {
    fn import_boxed(
        &self,
        source: &mut dyn Read,
        options: Box<dyn SerdeObj>,
        state: Box<dyn SerdeObj>,
    ) -> Result<BoxedImporterValue>;
    fn default_options(&self) -> Box<dyn SerdeObj>;
    fn default_state(&self) -> Box<dyn SerdeObj>;
    fn version(&self) -> u32;
    fn deserialize_metadata<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<SourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>>;
    fn deserialize_options<'a>(&self, bytes: &'a [u8]) -> Result<Box<dyn SerdeObj>>;
    fn deserialize_state<'a>(&self, bytes: &'a [u8]) -> Result<Box<dyn SerdeObj>>;
}
pub struct BoxedImporterValue {
    pub value: ImporterValue,
    pub options: Box<dyn SerdeObj>,
    pub state: Box<dyn SerdeObj>,
}

impl<S, O, T> BoxedImporter for T
where
    O: SerdeObj + Serialize + Default + Send + Sync + Clone + for<'a> Deserialize<'a>,
    S: SerdeObj + Serialize + Default + Send + Sync + for<'a> Deserialize<'a>,
    T: Importer<State = S, Options = O> + TypeUuid + Send + Sync,
{
    fn import_boxed(
        &self,
        source: &mut dyn Read,
        options: Box<dyn SerdeObj>,
        state: Box<dyn SerdeObj>,
    ) -> Result<BoxedImporterValue> {
        let mut s = state.downcast::<S>().unwrap();
        let o = *options.downcast::<O>().unwrap();
        let result = self.import(source, o.clone(), &mut s)?;
        Ok(BoxedImporterValue {
            value: result,
            options: Box::new(o),
            state: s,
        })
    }
    fn default_options(&self) -> Box<dyn SerdeObj> {
        Box::new(O::default())
    }
    fn default_state(&self) -> Box<dyn SerdeObj> {
        Box::new(S::default())
    }
    fn version(&self) -> u32 {
        self.version()
    }
    fn deserialize_metadata<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<SourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>> {
        let metadata: SourceMetadata<O, S> = ron::de::from_bytes(&bytes)?;
        Ok(SourceMetadata {
            version: metadata.version,
            import_hash: metadata.import_hash,
            importer_version: metadata.importer_version,
            importer_options: Box::new(metadata.importer_options),
            importer_state: Box::new(metadata.importer_state),
            assets: metadata.assets.clone(),
        })
    }
    fn deserialize_options<'a>(&self, bytes: &'a [u8]) -> Result<Box<dyn SerdeObj>> {
        Ok(Box::new(bincode::deserialize::<O>(&bytes)?))
    }
    fn deserialize_state<'a>(&self, bytes: &'a [u8]) -> Result<Box<dyn SerdeObj>> {
        Ok(Box::new(bincode::deserialize::<S>(&bytes)?))
    }
}

#[derive(Debug)]
pub struct SourceFileImporter {
    pub extension: &'static str,
    pub instantiator: fn() -> Box<dyn BoxedImporter>,
}
inventory::collect!(SourceFileImporter);

pub fn get_source_importers(
) -> impl Iterator<Item = (&'static str, Box<dyn BoxedImporter + 'static>)> {
    inventory::iter::<SourceFileImporter>
        .into_iter()
        .map(|s| (s.extension.trim_start_matches("."), (s.instantiator)()))
}
