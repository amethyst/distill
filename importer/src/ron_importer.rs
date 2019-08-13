use crate::{Importer, ImporterValue, SerdeObj, ImportedAsset, IntoSerdeObj, SourceFileImporter};
use atelier_core::{AssetUuid};
use serde::{Serialize, Deserialize};
use ron::de::from_reader;
use type_uuid::TypeUuid;
use std::io::Read;

#[typetag::serde(tag = "type_uuid")]
pub trait RonImportable: SerdeObj + IntoSerdeObj {}

/// A simple state for Importer to retain the same UUID between imports
/// for all single-asset source files
#[derive(Default, Deserialize, Serialize, TypeUuid)]
#[uuid = "fabe2809-dcc0-4463-b741-a456ca6b28ed"]
pub struct RonImporterState {
    id: Option<AssetUuid>,
}

#[derive(Default, TypeUuid)]
#[uuid = "162ede20-6fdd-44c1-8387-8f93983c067c"]
struct RonImporter{}

impl Importer for RonImporter {
    type State = RonImporterState;
    type Options = ();

    fn version_static() -> u32 {
        1
    }

    fn version(&self) -> u32 {
        Self::version_static()
    }

    fn import(
        &self,
        source: &mut dyn Read,
        _: Self::Options,
        state: &mut Self::State,
    ) -> crate::Result<ImporterValue> {
        if state.id.is_none() {
            state.id = Some(*uuid::Uuid::new_v4().as_bytes());
        }

        let de: Box<dyn RonImportable> = from_reader(source)?;

        Ok(ImporterValue {
            assets: vec![ImportedAsset {
                id: state.id.expect("AssetUuid not generated"),
                search_tags: Vec::new(),
                build_deps: Vec::new(),
                load_deps: Vec::new(),
                instantiate_deps: Vec::new(),
                asset_data: de.into_serde_obj(),
                build_pipeline: None,
             }],
        })
    }
}

inventory::submit!(SourceFileImporter {
    extension: ".ron",
    instantiator: || Box::<RonImporter>::new(Default::default())
});
