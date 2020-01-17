use crate::capnp_db::{DBTransaction, Environment, MessageReader, RwTransaction};
use crate::error::Result;
use crate::serialized_asset::SerializedAsset;
use atelier_schema::data::artifact;
use std::sync::Arc;

pub struct ArtifactCache {
    db: Arc<Environment>,
    tables: ArtifactCacheTables,
}

struct ArtifactCacheTables {
    /// Maps a hash to the serialized artifact data
    /// u64 -> Artifact
    hash_to_artifact: lmdb::Database,
}

impl ArtifactCache {
    pub fn new(db: &Arc<Environment>) -> Result<ArtifactCache> {
        Ok(ArtifactCache {
            db: db.clone(),
            tables: ArtifactCacheTables {
                hash_to_artifact: db.create_db(
                    Some("ArtifactCache::hash_to_artifact"),
                    lmdb::DatabaseFlags::INTEGER_KEY,
                )?,
            },
        })
    }

    pub fn delete(&self, txn: &mut RwTransaction<'_>, hash: u64) -> bool {
        txn.delete(self.tables.hash_to_artifact, &hash.to_le_bytes())
            .expect("db: Failed to delete entry from hash_to_artifact table")
    }

    pub fn insert<T: AsRef<[u8]>>(&self, txn: &mut RwTransaction<'_>, artifact: &SerializedAsset<T>) {
        txn.put(
            self.tables.hash_to_artifact,
            &artifact.metadata.hash.to_le_bytes(),
            &build_artifact_message(artifact),
        )
        .expect("lmdb: failed to put path ref");
    }

    pub fn get<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        hash: u64,
    ) -> Option<MessageReader<'a, artifact::Owned>> {
        txn.get::<artifact::Owned, _>(self.tables.hash_to_artifact, &hash.to_le_bytes())
            .expect("db: Failed to get entry from hash_to_artifact table")
    }

    // pub fn get_or_insert_with<'a, T: AsRef<[u8]>>(
    //     &self,
    //     txn: &'a mut RwTransaction,
    //     inserter: impl FnOnce() -> SerializedAsset<T>,
    // ) -> artifact::Reader<'a> {
    //     match self.get(txn) {
    //         Some(r) => r,
    //         None => {
    //             self.insert(txn, &inserter());
    //             self.get(txn).expect("Inserted in same transaction")
    //         }
    //     }
    // }
}

// deduplicate with asset_hub_service, move artifact building to cache only
fn build_artifact_message<T: AsRef<[u8]>>(
    artifact: &SerializedAsset<T>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<artifact::Builder<'_>>();
        let mut metadata = m.reborrow().init_metadata();
        crate::asset_hub::build_artifact_metadata(&artifact.metadata, &mut metadata);
        let slice: &[u8] = artifact.data.as_ref();
        m.reborrow().set_data(slice);
    }
    value_builder
}