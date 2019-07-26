use crate::capnp_db::{CapnpCursor, DBTransaction, Environment, MessageReader, RwTransaction};
use crate::error::Result;
use crate::utils;
use atelier_importer::{AssetMetadata, AssetUuid};
use atelier_schema::data::{
    self, asset_change_log_entry,
    asset_metadata::{self, latest_artifact},
};
use futures::sync::mpsc::Sender;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

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
    /// Maps an AssetID to its most recent metadata and artifact
    /// AssetUuid -> ImportedMetadata
    asset_metadata: lmdb::Database,
    /// Maps a SequenceNum to a AssetChangeLogEntry
    /// SequenceNum -> AssetChangeLogEntry
    asset_changes: lmdb::Database,
}

fn set_assetid_list(
    asset_ids: &[AssetUuid],
    builder: &mut capnp::struct_list::Builder<'_, data::asset_uuid::Owned>,
) {
    for (idx, uuid) in asset_ids.iter().enumerate() {
        builder.reborrow().get(idx as u32).set_id(uuid);
    }
}

fn build_asset_metadata<K>(
    metadata: &AssetMetadata,
    artifact_hash: Option<&[u8]>,
    source: data::AssetSource,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<asset_metadata::Builder<'_>>();
        m.reborrow().init_id().set_id(&metadata.id);
        if let Some(pipeline) = metadata.build_pipeline {
            m.reborrow().init_build_pipeline().set_id(&pipeline);
        }
        set_assetid_list(
            &metadata.load_deps,
            &mut m.reborrow().init_load_deps(metadata.load_deps.len() as u32),
        );
        set_assetid_list(
            &metadata.build_deps,
            &mut m
                .reborrow()
                .init_build_deps(metadata.build_deps.len() as u32),
        );
        set_assetid_list(
            &metadata.instantiate_deps,
            &mut m
                .reborrow()
                .init_instantiate_deps(metadata.instantiate_deps.len() as u32),
        );
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
        if let Some(artifact_hash) = artifact_hash {
            m.reborrow()
                .init_latest_artifact()
                .init_id()
                .set_hash(artifact_hash);
        }
        m.reborrow()
            .set_imported_asset_type(&metadata.import_asset_type);
        m.reborrow()
            .set_built_asset_type(&metadata.import_asset_type);
        m.reborrow().set_source(source);
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
                db_evt.reborrow().init_id().set_id(&evt.id);
                if let Some(ref import_hash) = evt.import_hash {
                    db_evt.reborrow().init_import_hash().set_hash(import_hash);
                }
                if let Some(ref build_dep_hash) = evt.build_dep_hash {
                    db_evt.reborrow().set_build_dep_hash(build_dep_hash);
                }
            }
            ChangeEvent::Remove(id) => {
                value.init_remove_event().init_id().set_id(id);
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
    ) -> Result<Option<MessageReader<'a, asset_metadata::Owned>>> {
        Ok(txn.get::<asset_metadata::Owned, _>(self.tables.asset_metadata, &id)?)
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
            list.reborrow().get(idx as u32).set_id(uuid);
        }
        txn.put(self.tables.build_dep_reverse, &id, &value_builder)?;
        Ok(())
    }

    pub fn update_asset(
        &self,
        txn: &mut RwTransaction<'_>,
        import_hash: u64,
        metadata: &AssetMetadata,
        source: data::AssetSource,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let hash_bytes = import_hash.to_le_bytes();
        let mut maybe_id = None;
        let existing_metadata: Option<MessageReader<'_, asset_metadata::Owned>> =
            txn.get(self.tables.asset_metadata, &metadata.id)?;
        let new_metadata = build_asset_metadata::<&[u8; 8]>(&metadata, Some(&hash_bytes), source);
        let mut deps_to_delete = Vec::new();
        let mut deps_to_add = Vec::new();
        if let Some(existing_metadata) = existing_metadata {
            let existing_metadata = existing_metadata.get()?;
            let latest_artifact = existing_metadata.get_latest_artifact();
            if let latest_artifact::Id(Ok(id)) = latest_artifact.which()? {
                maybe_id = Some(Vec::from(id.get_hash()?));
            }
            for dep in existing_metadata.get_build_deps()? {
                let dep = utils::uuid_from_slice(dep.get_id()?)?;
                if !metadata.build_deps.contains(&dep) {
                    deps_to_delete.push(dep);
                }
                deps_to_add
                    .iter()
                    .position(|x| x == &dep)
                    .map(|i| deps_to_add.swap_remove(i));
            }
        } else {
            deps_to_add.extend(metadata.build_deps.iter());
        }
        let mut artifact_changed = true;
        if let Some(id) = maybe_id.as_ref() {
            artifact_changed = hash_bytes != id.as_slice();
        }
        for dep in deps_to_add {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    dependees.push(utils::uuid_from_slice(uuid.get_id()?)?);
                }
            }
            dependees.push(metadata.id);
            self.put_build_deps_reverse(txn, &dep, dependees)?;
        }
        for dep in deps_to_delete {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    dependees.push(utils::uuid_from_slice(uuid.get_id()?)?);
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
        let metadata = self.get_metadata(txn, id)?;
        let mut deps_to_delete = Vec::new();
        if let Some(metadata) = metadata {
            let metadata = metadata.get()?;
            for dep in metadata.get_build_deps()? {
                deps_to_delete.push(utils::uuid_from_slice(dep.get_id()?)?);
            }
        }
        if txn.delete(self.tables.asset_metadata, &id)? {
            change_batch.content_changes.push(*id);
        }
        for dep in deps_to_delete {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    dependees.push(utils::uuid_from_slice(uuid.get_id()?)?);
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
                        to_check.push_back(utils::uuid_from_slice(dependee.get_id()?)?);
                    }
                }
            }
        }
        for asset in affected_assets {
            let metadata = self.get_metadata(txn, &asset)?;
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
                    let metadata = self.get_metadata(txn, &id)?;
                    if let Some(metadata) = metadata {
                        let metadata = metadata.get()?;
                        if let latest_artifact::Id(Ok(id)) =
                            metadata.get_latest_artifact().which()?
                        {
                            dependency_graph.insert(asset, Vec::from(id.get_hash()?));
                        }
                        for dep in metadata.get_build_deps()? {
                            to_check.push_back(utils::uuid_from_slice(dep.get_id()?)?);
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
                    if let latest_artifact::Id(Ok(id)) = metadata.get_latest_artifact().which()? {
                        Vec::from(id.get_hash()?)
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
            match listener.try_send(AssetBatchEvent::Commit) {
                Err(ref err) if err.is_disconnected() => {
                    to_remove.push(*id);
                }
                _ => {}
            }
        }
        for id in to_remove {
            listeners.remove(&id);
        }
    }

    pub fn register_listener(&self, listener: Sender<AssetBatchEvent>) -> ListenerID {
        let id = self.id_gen.fetch_add(1, Ordering::Relaxed);
        self.listeners.lock().unwrap().insert(id, listener);
        id
    }

    pub fn drop_listener(&self, listener: ListenerID) -> Option<Sender<AssetBatchEvent>> {
        self.listeners.lock().unwrap().remove(&listener)
    }
}
