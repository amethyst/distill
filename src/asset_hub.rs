use crate::capnp_db::{DBTransaction, Environment, MessageReader, RwTransaction};
use crate::error::Result;
use importer::{AssetMetadata, AssetUUID};
use log::debug;
use schema::data::{
    self,
    imported_metadata::{self, latest_artifact},
};
use std::sync::Arc;

pub struct AssetHub {
    // db: Arc<Environment>,
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
    asset_ids: &[AssetUUID],
    builder: &mut capnp::struct_list::Builder<data::asset_uuid::Owned>,
) {
    for (idx, uuid) in asset_ids.iter().enumerate() {
        builder.reborrow().get(idx as u32).set_id(uuid.as_bytes());
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
                m.reborrow().init_id().set_id(metadata.id.as_bytes());
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
            // db,
        })
    }

    pub fn get_metadata_iter<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
    ) -> Result<lmdb::RoCursor<'a>> {
        let cursor = txn.open_ro_cursor(self.tables.asset_metadata)?;
        Ok(cursor)
    }

    pub fn get_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        id: &AssetUUID,
    ) -> Result<Option<MessageReader<'a, imported_metadata::Owned>>> {
        Ok(txn.get::<imported_metadata::Owned, _>(self.tables.asset_metadata, id.as_bytes())?)
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
        let hash_bytes = import_hash.to_le_bytes();
        let mut maybe_id = None;
        let existing_metadata: Option<MessageReader<imported_metadata::Owned>> =
            txn.get(self.tables.asset_metadata, metadata.id.as_bytes())?;
        if let Some(existing_metadata) = existing_metadata {
            let latest_artifact = existing_metadata.get()?.get_latest_artifact();
            if let latest_artifact::Id(Ok(id)) = latest_artifact.which()? {
                maybe_id = Some(Vec::from(id.get_hash()?));
            }
        }
        if let Some(id) = maybe_id.as_ref() {
            if !id.is_empty() && hash_bytes != id.as_slice() {
                txn.delete(self.tables.import_artifacts, id)?;
                debug!("deleted artifact {:?} {:?}", hash_bytes, id.as_slice());
            }
        }
        let imported_metadata = build_imported_metadata::<&[u8; 8]>(
            &metadata,
            asset
                .as_ref()
                .map(|_| &hash_bytes as &[u8])
                .or_else(|| maybe_id.as_ref().map(|a| a.as_slice())),
        );
        debug!("hash {:?}", hash_bytes);
        txn.put(
            self.tables.asset_metadata,
            metadata.id.as_bytes(),
            &imported_metadata,
        )?;
        // if let Some(asset) = asset {
        // txn.put_bytes(self.tables.import_artifacts, &hash_bytes, &asset)?;
        //     debug!("put artifact {:?}", hash_bytes);
        // }
        Ok(())
    }

    pub fn remove_asset(&self, txn: &mut RwTransaction, id: &AssetUUID) -> Result<()> {
        let mut artifact_hash = None;
        let metadata = self.get_metadata(txn, id)?;
        if let Some(metadata) = metadata {
            if let latest_artifact::Id(Ok(id)) = metadata.get()?.get_latest_artifact().which()? {
                let hash = id.get_hash()?;
                if !hash.is_empty() {
                    artifact_hash = Some(Vec::from(hash));
                }
            }
        }
        if let Some(artifact_hash) = artifact_hash {
            txn.delete(self.tables.import_artifacts, &artifact_hash)?;
        }
        txn.delete(self.tables.asset_metadata, id.as_bytes())?;
        Ok(())
    }
}
