use crate::capnp_db::{CapnpCursor, DBTransaction, Environment, MessageReader, RwTransaction};
use crate::error::{Error, Result};
use atelier_core::{utils, AssetRef, AssetUuid};
use atelier_importer::{ArtifactMetadata, AssetMetadata};
use atelier_schema::data::{
    self, artifact_metadata, asset_change_log_entry,
    asset_metadata::{self, latest_artifact},
    asset_ref,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use async_channel::Sender;

pub type ListenerID = u64;

pub struct AssetHub {
    // db: Arc<Environment>,
    tables: AssetHubTables,
    id_gen: AtomicU64,
    listeners: Mutex<HashMap<ListenerID, Sender<AssetBatchEvent>>>,
}

struct AssetContentUpdateEvent {
    id: AssetUuid,
    import_hash: Option<Vec<u8>>,
    build_dep_hash: Option<Vec<u8>>,
}

enum ChangeEvent {
    ContentUpdate(AssetContentUpdateEvent),
    Remove(AssetUuid),
}

#[derive(Debug)]
pub enum AssetBatchEvent {
    Commit,
}

pub struct ChangeBatch {
    content_changes: Vec<AssetUuid>,
}

impl ChangeBatch {
    pub fn new() -> ChangeBatch {
        ChangeBatch {
            content_changes: Vec::new(),
        }
    }
}

struct AssetHubTables {
    /// Maps an AssetUuid to a list of other assets that have a build dependency on it
    /// AssetUuid -> [AssetUuid]
    build_dep_reverse: lmdb::Database,
    /// Maps an AssetID to its most recent metadata
    /// AssetUuid -> ImportedMetadata
    asset_metadata: lmdb::Database,
    /// Maps a SequenceNum to a AssetChangeLogEntry
    /// SequenceNum -> AssetChangeLogEntry
    asset_changes: lmdb::Database,
}

fn set_assetref_list(
    asset_ids: &[AssetRef],
    builder: &mut capnp::struct_list::Builder<'_, data::asset_ref::Owned>,
) {
    for (idx, asset_ref) in asset_ids.iter().enumerate() {
        let mut builder = builder.reborrow().get(idx as u32);
        match asset_ref {
            AssetRef::Path(p) => {
                builder.set_path(
                    p.to_str()
                        .expect("failed to convert path to string")
                        .as_bytes(),
                );
            }
            AssetRef::Uuid(uuid) => {
                builder.init_uuid().set_id(&uuid.0);
            }
        }
    }
}

pub(crate) fn parse_db_asset_ref(asset_ref: &asset_ref::Reader<'_>) -> AssetRef {
    match asset_ref.which().expect("capnp: failed to read asset ref") {
        asset_ref::Path(p) => AssetRef::Path(PathBuf::from(
            std::str::from_utf8(p.expect("capnp: failed to read asset ref path"))
                .expect("capnp: failed to parse utf8 string"),
        )),
        asset_ref::Uuid(uuid) => AssetRef::Uuid(utils::make_array(
            uuid.and_then(|id| id.get_id())
                .expect("capnp: failed to read asset ref uuid"),
        )),
    }
}

pub fn parse_artifact_metadata(artifact: &artifact_metadata::Reader<'_>) -> ArtifactMetadata {
    let asset_id = utils::make_array(
        artifact
            .get_asset_id()
            .expect("capnp: failed to read asset_id")
            .get_id()
            .expect("capnp: failed to read asset_id"),
    );
    let compressed_size = artifact.get_compressed_size();
    let compressed_size = if compressed_size == 0 {
        None
    } else {
        Some(compressed_size)
    };
    let uncompressed_size = artifact.get_uncompressed_size();
    let uncompressed_size = if uncompressed_size == 0 {
        None
    } else {
        Some(uncompressed_size)
    };
    ArtifactMetadata {
        id: asset_id,
        hash: u64::from_le_bytes(utils::make_array(
            artifact.get_hash().expect("capnp: failed to read hash"),
        )),
        load_deps: artifact
            .get_load_deps()
            .expect("capnp: failed to read load deps")
            .iter()
            .map(|dep| parse_db_asset_ref(&dep))
            .collect(),
        build_deps: artifact
            .get_build_deps()
            .expect("capnp: failed to read build deps")
            .iter()
            .map(|dep| parse_db_asset_ref(&dep))
            .collect(),
        type_id: utils::make_array(
            artifact
                .get_type_id()
                .expect("capnp: failed to read asset type"),
        ),
        compression: artifact
            .get_compression()
            .expect("capnp: failed to read compression type")
            .into(),
        compressed_size,
        uncompressed_size,
    }
}

pub fn parse_db_metadata(metadata: &asset_metadata::Reader<'_>) -> AssetMetadata {
    let asset_id = utils::make_array(
        metadata
            .get_id()
            .expect("capnp: failed to read asset_id")
            .get_id()
            .expect("capnp: failed to read asset_id"),
    );
    let search_tags = metadata
        .get_search_tags()
        .expect("capnp: failed to read search tags")
        .iter()
        .map(|tag| {
            let key =
                std::str::from_utf8(tag.get_key().expect("capnp: failed to read search tag key"))
                    .expect("failed to read tag key as utf8")
                    .to_owned();
            let value = std::str::from_utf8(
                tag.get_value()
                    .expect("capnp: failed to read search tag value"),
            )
            .expect("failed to read tag value as utf8")
            .to_owned();
            if !value.is_empty() {
                (key, Some(value))
            } else {
                (key, None)
            }
        })
        .collect();
    let artifact_metadata = if let latest_artifact::Artifact(Ok(artifact)) = metadata
        .get_latest_artifact()
        .which()
        .expect("capnp: failed to read latest_artifact")
    {
        Some(parse_artifact_metadata(&artifact))
    } else {
        None
    };
    let build_pipeline = metadata
        .get_build_pipeline()
        .expect("capnp: failed to read build pipeline")
        .get_id()
        .expect("capnp: failed to read build pipeline id");
    let build_pipeline = if !build_pipeline.is_empty() {
        Some(utils::make_array(build_pipeline))
    } else {
        None
    };
    AssetMetadata {
        id: asset_id,
        search_tags,
        build_pipeline,
        artifact: artifact_metadata,
    }
}
pub(crate) fn build_artifact_metadata(
    artifact_metadata: &ArtifactMetadata,
    artifact: &mut artifact_metadata::Builder<'_>,
) {
    let mut artifact = artifact.reborrow();
    artifact
        .reborrow()
        .init_asset_id()
        .set_id(&artifact_metadata.id.0);
    artifact
        .reborrow()
        .set_hash(&artifact_metadata.hash.to_le_bytes());
    set_assetref_list(
        &artifact_metadata.load_deps,
        &mut artifact
            .reborrow()
            .init_load_deps(artifact_metadata.load_deps.len() as u32),
    );
    set_assetref_list(
        &artifact_metadata.build_deps,
        &mut artifact
            .reborrow()
            .init_build_deps(artifact_metadata.build_deps.len() as u32),
    );
    artifact
        .reborrow()
        .set_compression(artifact_metadata.compression.into());
    artifact
        .reborrow()
        .set_compressed_size(artifact_metadata.compressed_size.unwrap_or(0));
    artifact
        .reborrow()
        .set_uncompressed_size(artifact_metadata.uncompressed_size.unwrap_or(0));
    artifact
        .reborrow()
        .set_type_id(&artifact_metadata.type_id.0);
}

pub(crate) fn build_asset_metadata(
    metadata: &AssetMetadata,
    m: &mut asset_metadata::Builder<'_>,
    source: data::AssetSource,
) {
    m.reborrow().init_id().set_id(&metadata.id.0);
    if let Some(pipeline) = metadata.build_pipeline {
        m.reborrow().init_build_pipeline().set_id(&pipeline.0);
    }
    let mut search_tags = m
        .reborrow()
        .init_search_tags(metadata.search_tags.len() as u32);
    for (idx, (key, value)) in metadata.search_tags.iter().enumerate() {
        search_tags
            .reborrow()
            .get(idx as u32)
            .set_key(key.as_bytes());
        if let Some(value) = value {
            search_tags
                .reborrow()
                .get(idx as u32)
                .set_value(value.as_bytes());
        }
    }
    if let Some(artifact_metadata) = &metadata.artifact {
        let mut artifact = m.reborrow().init_latest_artifact().init_artifact();
        build_artifact_metadata(&artifact_metadata, &mut artifact);
    } else {
        m.reborrow().init_latest_artifact().set_none(());
    }
    m.reborrow().set_source(source);
}

fn build_asset_metadata_message<K>(
    metadata: &AssetMetadata,
    source: data::AssetSource,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<asset_metadata::Builder<'_>>();
        build_asset_metadata(metadata, &mut m, source);
    }
    value_builder
}

fn add_asset_changelog_entry(
    tables: &AssetHubTables,
    txn: &mut RwTransaction<'_>,
    change: &ChangeEvent,
) -> Result<()> {
    let mut last_seq: u64 = 0;
    let last_element = txn
        .open_ro_cursor(tables.asset_changes)?
        .capnp_iter_start()
        .last();
    if let Some((key, _)) = last_element {
        last_seq = u64::from_le_bytes(utils::make_array(key));
    }
    last_seq += 1;
    let mut value_builder = capnp::message::Builder::new_default();
    let mut value = value_builder.init_root::<asset_change_log_entry::Builder<'_>>();
    value.reborrow().set_num(last_seq);
    {
        let value = value.reborrow().init_event();
        match change {
            ChangeEvent::ContentUpdate(evt) => {
                let mut db_evt = value.init_content_update_event();
                db_evt.reborrow().init_id().set_id(&evt.id.0);
                if let Some(ref import_hash) = evt.import_hash {
                    db_evt.reborrow().set_import_hash(import_hash);
                }
                if let Some(ref build_dep_hash) = evt.build_dep_hash {
                    db_evt.reborrow().set_build_dep_hash(build_dep_hash);
                }
            }
            ChangeEvent::Remove(id) => {
                value.init_remove_event().init_id().set_id(&id.0);
            }
        }
    }
    txn.put(
        tables.asset_changes,
        &last_seq.to_le_bytes(),
        &value_builder,
    )?;
    Ok(())
}

impl AssetHub {
    pub fn new(db: Arc<Environment>) -> Result<AssetHub> {
        Ok(AssetHub {
            tables: AssetHubTables {
                asset_metadata: db
                    .create_db(Some("asset_metadata"), lmdb::DatabaseFlags::default())?,
                build_dep_reverse: db
                    .create_db(Some("build_dep_reverse"), lmdb::DatabaseFlags::default())?,
                asset_changes: db
                    .create_db(Some("asset_changes"), lmdb::DatabaseFlags::INTEGER_KEY)?,
            },
            id_gen: AtomicU64::new(1),
            listeners: Mutex::new(HashMap::new()),
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
        id: &AssetUuid,
    ) -> Option<MessageReader<'a, asset_metadata::Owned>> {
        txn.get::<asset_metadata::Owned, _>(self.tables.asset_metadata, &id)
            .expect("db: failed to get asset_metadata")
    }

    pub fn get_build_deps_reverse<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        id: &AssetUuid,
    ) -> Result<Option<MessageReader<'a, data::asset_uuid_list::Owned>>> {
        Ok(txn.get::<data::asset_uuid_list::Owned, _>(self.tables.build_dep_reverse, &id)?)
    }

    fn put_build_deps_reverse(
        &self,
        txn: &mut RwTransaction<'_>,
        id: &AssetUuid,
        dependees: Vec<AssetUuid>,
    ) -> Result<()> {
        let mut value_builder = capnp::message::Builder::new_default();
        let mut value = value_builder.init_root::<data::asset_uuid_list::Builder<'_>>();
        let mut list = value.reborrow().init_list(dependees.len() as u32);
        for (idx, uuid) in dependees.iter().enumerate() {
            list.reborrow().get(idx as u32).set_id(&uuid.0);
        }
        txn.put(self.tables.build_dep_reverse, &id, &value_builder)?;
        Ok(())
    }

    pub fn update_asset(
        &self,
        txn: &mut RwTransaction<'_>,
        metadata: &AssetMetadata,
        source: data::AssetSource,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let existing_metadata: Option<MessageReader<'_, asset_metadata::Owned>> =
            txn.get(self.tables.asset_metadata, &metadata.id)?;
        let new_metadata = build_asset_metadata_message::<&[u8; 8]>(&metadata, source);
        let mut deps_to_delete = Vec::new();
        let mut deps_to_add = Vec::new();
        let mut artifact_changed = true;
        if let Some(artifact_metadata) = &metadata.artifact {
            if let Some(existing_metadata) = existing_metadata {
                let existing_metadata = existing_metadata.get()?;
                let latest_artifact = existing_metadata.get_latest_artifact();
                let mut existing_deps = HashSet::new();
                if let latest_artifact::Artifact(Ok(artifact)) = latest_artifact.which()? {
                    artifact_changed =
                        artifact_metadata.hash.to_le_bytes() != artifact.get_hash()?;
                    for dep in artifact.get_build_deps()? {
                        let dep = *parse_db_asset_ref(&dep).expect_uuid();
                        existing_deps.insert(dep);
                        if !artifact_metadata.build_deps.contains(&AssetRef::Uuid(dep)) {
                            deps_to_delete.push(dep);
                        }
                    }
                }
                for dep in artifact_metadata.build_deps.iter() {
                    if !existing_deps.contains(dep.expect_uuid()) {
                        deps_to_add.push(dep);
                    }
                }
            } else {
                deps_to_add.extend(&artifact_metadata.build_deps);
            }
        }
        for dep in deps_to_add {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, dep.expect_uuid())? {
                for uuid in existing_list.get()?.get_list()? {
                    let uuid = utils::uuid_from_slice(uuid.get_id()?).ok_or(Error::UuidLength)?;
                    dependees.push(uuid);
                }
            }
            dependees.push(metadata.id);
            self.put_build_deps_reverse(txn, dep.expect_uuid(), dependees)?;
        }
        for dep in deps_to_delete {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    let uuid = utils::uuid_from_slice(uuid.get_id()?).ok_or(Error::UuidLength)?;
                    dependees.push(uuid);
                }
            }
            dependees
                .iter()
                .position(|x| x == &metadata.id)
                .map(|i| dependees.swap_remove(i));
            if dependees.is_empty() {
                txn.delete(self.tables.build_dep_reverse, &dep)?;
            } else {
                self.put_build_deps_reverse(txn, &dep, dependees)?;
            }
        }
        txn.put(self.tables.asset_metadata, &metadata.id, &new_metadata)?;
        if artifact_changed {
            change_batch.content_changes.push(metadata.id);
        }
        Ok(())
    }

    pub fn remove_asset(
        &self,
        txn: &mut RwTransaction<'_>,
        id: &AssetUuid,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let metadata = self.get_metadata(txn, id);
        let mut deps_to_delete = Vec::new();
        if let Some(metadata) = metadata {
            let metadata = metadata.get()?;
            if let latest_artifact::Artifact(Ok(artifact)) =
                metadata.get_latest_artifact().which()?
            {
                for dep in artifact.get_build_deps()? {
                    deps_to_delete.push(*parse_db_asset_ref(&dep).expect_uuid());
                }
            }
        }
        if txn.delete(self.tables.asset_metadata, &id)? {
            change_batch.content_changes.push(*id);
        }
        for dep in deps_to_delete {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    let uuid = utils::uuid_from_slice(uuid.get_id()?).ok_or(Error::UuidLength)?;
                    dependees.push(uuid);
                }
            }
            dependees
                .iter()
                .position(|x| x == id)
                .map(|i| dependees.swap_remove(i));
            if dependees.is_empty() {
                txn.delete(self.tables.build_dep_reverse, &dep)?;
            } else {
                self.put_build_deps_reverse(txn, &dep, dependees)?;
            }
        }
        Ok(())
    }

    pub fn add_changes(
        &self,
        txn: &mut RwTransaction<'_>,
        change_batch: ChangeBatch,
    ) -> Result<bool> {
        // TODO find the set of all changed assets, check the build dependency index and emit changes for all
        // assets that have changed and all the assets where the build_dep_hash has changed.
        // dedupe change events
        let mut to_check = VecDeque::new();
        let mut affected_assets = HashSet::new();
        let mut events = Vec::new();
        for id in change_batch.content_changes {
            to_check.push_back(id);
        }
        if !to_check.is_empty() {
            log::info!("{} assets changed content", to_check.len());
        }
        while !to_check.is_empty() {
            let id = to_check.pop_front().unwrap();
            if affected_assets.insert(id) {
                if let Some(dependees) = self.get_build_deps_reverse(txn, &id)? {
                    for dependee in dependees.get()?.get_list()? {
                        let uuid =
                            utils::uuid_from_slice(dependee.get_id()?).ok_or(Error::UuidLength)?;
                        to_check.push_back(uuid);
                    }
                }
            }
        }
        for asset in affected_assets {
            let metadata = self.get_metadata(txn, &asset);
            if let Some(metadata) = metadata {
                let metadata = metadata.get()?;
                let mut dependency_graph = HashMap::new();
                let mut to_check = VecDeque::new();
                to_check.push_back(asset);
                while !to_check.is_empty() {
                    let id = to_check.pop_front().unwrap();
                    if dependency_graph.contains_key(&id) {
                        continue;
                    }
                    let metadata = self.get_metadata(txn, &id);
                    if let Some(metadata) = metadata {
                        let metadata = metadata.get()?;
                        if let latest_artifact::Artifact(Ok(artifact)) =
                            metadata.get_latest_artifact().which()?
                        {
                            dependency_graph.insert(asset, Vec::from(artifact.get_hash()?));
                            for dep in artifact.get_build_deps()? {
                                to_check.push_back(*parse_db_asset_ref(&dep).expect_uuid());
                            }
                        }
                    }
                }
                let mut sorted_assets: Vec<(&AssetUuid, &Vec<u8>)> =
                    dependency_graph.iter().collect();
                sorted_assets.sort_by(|(x, _), (y, _)| {
                    x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                });
                let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
                for (_, import_hash) in sorted_assets {
                    import_hash.hash(&mut hasher);
                }
                let build_dep_hash = hasher.finish();
                let import_hash = {
                    if let latest_artifact::Artifact(Ok(artifact)) =
                        metadata.get_latest_artifact().which()?
                    {
                        Vec::from(artifact.get_hash()?)
                    } else {
                        Vec::new()
                    }
                };
                events.push(ChangeEvent::ContentUpdate(AssetContentUpdateEvent {
                    id: asset,
                    import_hash: Some(import_hash),
                    build_dep_hash: Some(Vec::from(&build_dep_hash.to_le_bytes() as &[u8])),
                }));
            } else {
                events.push(ChangeEvent::Remove(asset));
            }
        }
        if !events.is_empty() {
            log::info!("{} asset events generated", events.len());
        }
        for event in events.iter() {
            add_asset_changelog_entry(&self.tables, txn, event)?;
        }
        Ok(!events.is_empty())
    }

    pub fn get_latest_asset_change<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
    ) -> Result<u64> {
        let mut last_seq: u64 = 0;
        let last_element = txn
            .open_ro_cursor(self.tables.asset_changes)?
            .capnp_iter_start()
            .last();
        if let Some((key, _)) = last_element {
            last_seq = u64::from_le_bytes(utils::make_array(key));
        }
        Ok(last_seq)
    }

    pub fn get_asset_changes_iter<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
    ) -> Result<lmdb::RoCursor<'a>> {
        let cursor = txn.open_ro_cursor(self.tables.asset_changes)?;
        Ok(cursor)
    }

    pub fn notify_listeners(&self) {
        let listeners = &mut *self.listeners.lock().unwrap();
        let mut to_remove = Vec::new();
        for (id, listener) in listeners.iter_mut() {
            if listener.try_send(AssetBatchEvent::Commit).is_err() {
                to_remove.push(*id);
            }
        }
        for id in to_remove {
            listeners.remove(&id);
        }
    }

    pub fn register_listener(
        &self,
        listener: Sender<AssetBatchEvent>,
    ) -> ListenerID {
        let id = self.id_gen.fetch_add(1, Ordering::Relaxed);
        self.listeners.lock().unwrap().insert(id, listener);
        id
    }

    pub fn drop_listener(
        &self,
        listener: ListenerID,
    ) -> Option<Sender<AssetBatchEvent>> {
        self.listeners.lock().unwrap().remove(&listener)
    }
}
