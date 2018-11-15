use amethyst::assets::AssetUUID;
use asset_import::AssetMetadata;
use capnp_db::{DBTransaction, Environment, RwTransaction};
use crossbeam_channel::{self as channel, Receiver};
use data_capnp::{
    self, asset_metadata, import_artifact_key,
    imported_metadata::{self, latest_artifact},
};
use error::Result;
use rayon::prelude::*;
use ron;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::{
    ffi::OsStr,
    fs,
    hash::{Hash, Hasher},
    io::BufRead,
    io::{Read, Write},
    iter::FromIterator,
    path::PathBuf,
    sync::Arc,
};

pub struct AssetHub {
    db: Arc<Environment>,
    tables: AssetHubTables,
}

struct AssetHubTables {
    /// Maps the hash of (source, meta, importer_version, importer_options) to an import artifact
    /// ImportArtifactKey -> [u8]
    import_artifacts: lmdb::Database,
    /// Maps an AssetID to its most recent metadata and artifact
    /// AssetID -> ImportedMetadata
    asset_metadata: lmdb::Database,
}
fn set_assetid_list(
    asset_ids: &Vec<AssetUUID>,
    builder: &mut capnp::struct_list::Builder<data_capnp::asset_id::Owned>,
) {
    for (idx, value) in asset_ids.iter().enumerate() {
        builder.reborrow().get(idx as u32).set_id(value);
    }
}
fn build_imported_metadata<K>(
    metadata: &AssetMetadata,
    artifact_hash: Option<&[u8]>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<imported_metadata::Builder>();
        {
            let mut m = value.reborrow().init_metadata();
            {
                m.reborrow().init_id().set_id(&metadata.id);
            }
            {
                set_assetid_list(
                    &metadata.load_deps,
                    &mut m.reborrow().init_load_deps(metadata.load_deps.len() as u32),
                );
            }
            {
                set_assetid_list(
                    &metadata.build_deps,
                    &mut m
                        .reborrow()
                        .init_build_deps(metadata.build_deps.len() as u32),
                );
            }
            {
                set_assetid_list(
                    &metadata.instantiate_deps,
                    &mut m
                        .reborrow()
                        .init_instantiate_deps(metadata.instantiate_deps.len() as u32),
                );
            }
        }
        {
            if let Some(artifact_hash) = artifact_hash {
                value
                    .reborrow()
                    .init_latest_artifact()
                    .init_id()
                    .set_hash(artifact_hash);
            }
        }
    }
    value_builder
}

impl AssetHub {
    pub fn new(db: Arc<Environment>) -> Result<AssetHub> {
        Ok(AssetHub {
            tables: AssetHubTables {
                import_artifacts: db
                    .create_db(Some("import_artifacts"), lmdb::DatabaseFlags::default())?,
                asset_metadata: db
                    .create_db(Some("asset_metadata"), lmdb::DatabaseFlags::default())?,
            },
            db,
        })
    }

    pub fn update_asset<A>(
        &self,
        txn: &mut RwTransaction,
        import_hash: u64,
        metadata: &AssetMetadata,
        asset: Option<A>,
    ) -> Result<()>
    where
        A: AsRef<[u8]>,
    {
        let hash_bytes: [u8; 8] = unsafe { std::mem::transmute(import_hash) };
        let sliced_hash: &[u8] = &hash_bytes;
        let mut maybe_id = None;
        {
            {
                let existing_metadata = txn.get(self.tables.asset_metadata, &metadata.id)?;
                if let Some(existing_metadata) = existing_metadata {
                    let existing_metadata =
                        existing_metadata.get_root::<imported_metadata::Reader>()?;
                    let latest_artifact = existing_metadata.get_latest_artifact();
                    if let latest_artifact::Id(Ok(id)) = latest_artifact.which()? {
                        maybe_id = Some(Vec::from(id.get_hash()?));
                    }
                }
            }
            if let Some(id) = maybe_id.as_ref() {
                if !id.is_empty() && hash_bytes != id.as_slice() {
                    txn.delete(self.tables.import_artifacts, id)?;
                    println!("deleted artifact {:?} {:?}", hash_bytes, id.as_slice());
                }
            }
        }
        {
            let imported_metadata = build_imported_metadata::<&[u8; 8]>(
                &metadata,
                asset
                    .as_ref()
                    .map(|_| sliced_hash)
                    .or(maybe_id.as_ref().map(|a| a.as_slice())),
            );
            println!("hash {:?}", hash_bytes);
            txn.put(self.tables.asset_metadata, &metadata.id, &imported_metadata)?;
        }
        // if let Some(asset) = asset {
            // txn.put_bytes(self.tables.import_artifacts, &hash_bytes, &asset)?;
        //     println!("put artifact {:?}", hash_bytes);
        // }
        Ok(())
    }

    pub fn remove_asset(&self, txn: &mut RwTransaction, id: AssetUUID) -> Result<()> {
        Ok(())
    }
}
