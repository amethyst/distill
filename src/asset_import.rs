use amethyst::assets::{Importer, ImporterValue, SimpleImporter, AssetUUID};
use serde::Deserialize;
use std::{io::Read};
use ron;

pub use amethyst::assets::SerdeObj;

#[derive(Clone, Serialize, Deserialize, Hash)]
pub struct AssetMetadata {
    pub id: AssetUUID,
    pub search_tags: Vec<(String, Option<String>)>,
    pub build_deps: Vec<AssetUUID>,
    pub load_deps: Vec<AssetUUID>,
    pub instantiate_deps: Vec<AssetUUID>,
}
pub const SOURCEMETADATA_VERSION: u32 = 1;
#[derive(Serialize, Deserialize)]
pub struct SourceMetadata<Options, State> {
    /// Metadata struct version
    pub version: u32,
    /// Hash of the source file this metadata was generated from
    pub source_hash: u64,
    pub importer_version: u32,
    pub importer_options: Options,
    pub importer_state: State,
    pub assets: Vec<AssetMetadata>,
}


pub trait BoxedImporter {
    fn import_boxed(
        &self,
        source: &mut Read,
        options: Box<SerdeObj>,
        state: Box<SerdeObj>,
    ) -> ::amethyst::assets::Result<BoxedImporterValue>;
    fn default_options(&self) -> Box<SerdeObj>;
    fn default_state(&self) -> Box<SerdeObj>;
    fn version(&self) -> u32;
    fn deserialize_metadata<'a>(&self, bytes: &'a [u8]) -> SourceMetadata<Box<SerdeObj>, Box<SerdeObj>>;
}
pub struct BoxedImporterValue {
    pub value: ImporterValue,
    pub options: Box<SerdeObj>,
    pub state: Box<SerdeObj>,
}

impl<S, O, T> BoxedImporter for T
where O: SerdeObj + Default + Send + Sync + Clone + for<'a> Deserialize<'a>,
S: SerdeObj + Default + Send + Sync + for<'a> Deserialize<'a>,
T: Importer<State = S, Options = O>

 {
    fn import_boxed(
        &self,
        source: &mut Read,
        options: Box<SerdeObj>,
        state: Box<SerdeObj>,
    ) -> ::amethyst::assets::Result<BoxedImporterValue> {
        let mut s = state.downcast::<S>().unwrap();
        let o = *options.downcast::<O>().unwrap();
        let result = self.import(source, o.clone(), &mut s)?;
        Ok(BoxedImporterValue {
            value: result,
            options: Box::new(o),
            state: s,
        })
    }
    fn default_options(&self) -> Box<SerdeObj> {
        Box::new(O::default())
    }
    fn default_state(&self) -> Box<SerdeObj> {
        Box::new(S::default())
    }
    fn version(&self) -> u32 {
        self.version()
    }
    fn deserialize_metadata<'a>(&self, bytes: &'a [u8]) -> SourceMetadata<Box<SerdeObj>, Box<SerdeObj>>
     {
        let metadata: SourceMetadata<O, S> = ron::de::from_bytes(&bytes).unwrap();
        SourceMetadata {
            version: metadata.version,
            source_hash: metadata.source_hash,
            importer_version: metadata.importer_version,
            importer_options: Box::new(metadata.importer_options),
            importer_state: Box::new(metadata.importer_state),
            assets: metadata.assets.clone(),
        }
    }
}

pub fn format_from_ext(ext: &str) -> Option<Box<BoxedImporter>> {
    match ext {
        "jpg" => Some(Box::new(
            SimpleImporter::from(::amethyst::renderer::JpgFormat {}),
        )),
        "png" => Some(Box::new(
            SimpleImporter::from(::amethyst::renderer::PngFormat {}),
        )),
        "tga" => Some(Box::new(
            SimpleImporter::from(::amethyst::renderer::TgaFormat {}),
        )),
        "bmp" => Some(Box::new(
            SimpleImporter::from(::amethyst::renderer::BmpFormat {}),
        )),
        "obj" => Some(Box::new(
            SimpleImporter::from(::amethyst::renderer::ObjFormat {}),
        )),
        "wav" => Some(Box::new(
            SimpleImporter::from(::amethyst::audio::WavFormat {}),
        )),
        "ogg" => Some(Box::new(
            SimpleImporter::from(::amethyst::audio::OggFormat {}),
        )),
        "flac" => Some(Box::new(
            SimpleImporter::from(::amethyst::audio::FlacFormat {}),
        )),
        "mp3" => Some(Box::new(
            SimpleImporter::from(::amethyst::audio::Mp3Format {}),
        )),
        _ => None,
    }
}
