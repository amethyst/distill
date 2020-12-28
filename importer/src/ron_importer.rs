use crate::{ImportOp, ImportedAsset, Importer, ImporterValue, Result, SerdeImportable};
use atelier_core::AssetUuid;
use ron::de::from_reader;
use serde::{Deserialize, Serialize};
use std::io::Read;
use type_uuid::*;

#[derive(Default, Deserialize, Serialize, TypeUuid, Clone, Copy)]
#[uuid = "f3cd048a-2c98-4e4b-95a2-d7c0ee6f7beb"]
pub struct RonImporterOptions {}

/// A simple state for Importer to retain the same UUID between imports
/// for all single-asset source files
#[derive(Default, Deserialize, Serialize, TypeUuid)]
#[uuid = "fabe2809-dcc0-4463-b741-a456ca6b28ed"]
pub struct RonImporterState {
    pub id: Option<AssetUuid>,
}

#[derive(Default, TypeUuid)]
#[uuid = "162ede20-6fdd-44c1-8387-8f93983c067c"]
pub struct RonImporter;

impl Importer for RonImporter {
    type Options = RonImporterOptions;
    type State = RonImporterState;

    fn version_static() -> u32 {
        1
    }

    fn version(&self) -> u32 {
        Self::version_static()
    }

    fn import(
        &self,
        _op: &mut ImportOp,
        source: &mut dyn Read,
        _: &Self::Options,
        state: &mut Self::State,
    ) -> Result<ImporterValue> {
        if state.id.is_none() {
            state.id = Some(AssetUuid(*uuid::Uuid::new_v4().as_bytes()));
        }
        let de: Box<dyn SerdeImportable> = from_reader(source)?;

        Ok(ImporterValue {
            assets: vec![ImportedAsset {
                id: state.id.expect("AssetUuid not generated"),
                search_tags: Vec::new(),
                build_deps: Vec::new(),
                load_deps: Vec::new(),
                asset_data: de.into_serde_obj(),
                build_pipeline: None,
            }],
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate as atelier_importer;
    use crate::*;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, TypeUuid, SerdeImportable, PartialEq, Eq)]
    #[uuid = "36fb2083-7195-4583-8af9-0965f10ae60d"]
    struct A {
        x: u32,
    }

    #[derive(Serialize, Deserialize, TypeUuid, SerdeImportable, PartialEq)]
    #[uuid = "d4b83227-d3f8-47f5-b026-db615fb41d31"]
    struct B {
        s: String,
        a: A,
        m: HashMap<String, String>,
    }

    #[test]
    fn ron_importer_simple_test() {
        let importer: Box<dyn BoxedImporter> = Box::new(RonImporter::default());

        let mut a = "{
                       \"36fb2083-7195-4583-8af9-0965f10ae60d\":
                        (
                           x: 30,
                        )
                     }"
        .as_bytes();

        let a_boxed_res = futures_executor::block_on(importer.import_boxed(
            &mut a,
            Box::new(RonImporterOptions {}),
            Box::new(RonImporterState { id: None }),
        ))
        .unwrap();
        let a_serde_obj = a_boxed_res
            .value
            .assets
            .into_iter()
            .nth(0)
            .unwrap()
            .asset_data;

        let a_downcast = a_serde_obj.any().downcast_ref::<A>();
        match a_downcast {
            Some(a) => assert_eq!(a.x, 30),
            None => panic!("Expected serde_obj to be downcast to `A`."),
        }
    }

    #[test]
    fn ron_importer_complex_test() {
        let importer: Box<dyn BoxedImporter> = Box::new(RonImporter::default());

        let mut b = "{
                       \"d4b83227-d3f8-47f5-b026-db615fb41d31\":
                        (
                            s: \"Ferris\",
                            a: (
                                x: 30
                               ),
                            m: {
                                \"lorem\": \"ipsum\",
                                \"dolor\": \"sim\",
                            }
                        )
                     }"
        .as_bytes();

        let mut op = ImportOp::default();
        let b_boxed_res = futures_executor::block_on(importer.import_boxed(
            &mut op,
            &mut b,
            Box::new(RonImporterOptions {}),
            Box::new(RonImporterState { id: None }),
        ))
        .unwrap();
        let b_serde_obj = b_boxed_res
            .value
            .assets
            .into_iter()
            .nth(0)
            .unwrap()
            .asset_data;
        let b_downcast = b_serde_obj.any().downcast_ref::<B>();
        match b_downcast {
            Some(b) => {
                assert_eq!(b.s, "Ferris");
                assert_eq!(b.a.x, 30);
                assert_eq!(b.m["lorem"], "ipsum");
                assert_eq!(b.m["dolor"], "sim");
                assert_eq!(b.m.len(), 2);
            }
            None => panic!("Expected serde_obj to be downcast to `B`."),
        }
    }
}
