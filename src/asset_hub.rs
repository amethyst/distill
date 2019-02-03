use crate::capnp_db::{CapnpCursor, DBTransaction, Environment, MessageReader, RwTransaction};
use crate::error::Result;
use crate::utils;
use futures::sync::mpsc::Sender;
use importer::{AssetMetadata, AssetUUID};
use log::debug;
use schema::data::{
    self, asset_change_log_entry,
    imported_metadata::{self, latest_artifact},
};
use slotmap::SlotMap;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

pub type ListenerID = slotmap::DefaultKey;

pub struct AssetHub {
    // db: Arc<Environment>,
    tables: AssetHubTables,
    listeners: Mutex<SlotMap<ListenerID, Sender<AssetBatchEvent>>>,
}

struct AssetContentUpdateEvent {
    id: AssetUUID,
    import_hash: Option<Vec<u8>>,
    build_dep_hash: Option<Vec<u8>>,
}

enum ChangeEvent {
    ContentUpdate(AssetContentUpdateEvent),
    Remove(AssetUUID),
}

pub enum AssetBatchEvent {
    Commit,
}

pub struct ChangeBatch {
    content_changes: Vec<AssetUUID>,
}

impl ChangeBatch {
    pub fn new() -> ChangeBatch {
        ChangeBatch {
            content_changes: Vec::new(),
        }
    }
}

struct AssetHubTables {
    /// Maps an AssetUUID to a list of other assets that have a build dependency on it
    /// AssetUUID -> [AssetUUID]
    build_dep_reverse: lmdb::Database,
    /// Maps an AssetID to its most recent metadata and artifact
    /// AssetUUID -> ImportedMetadata
    asset_metadata: lmdb::Database,
    /// Maps a SequenceNum to a AssetChangeLogEntry
    /// SequenceNum -> AssetChangeLogEntry
    asset_changes: lmdb::Database,
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
    source: data::AssetSource,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<imported_metadata::Builder>();
        {
            let mut m = value.reborrow().init_metadata();
            {
                m.reborrow().init_id().set_id(metadata.id.as_bytes());
                if let Some(pipeline) = metadata.build_pipeline {
                    m.reborrow()
                        .init_build_pipeline()
                        .set_id(pipeline.as_bytes());
                }
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
        value.reborrow().set_source(source);
    }
    value_builder
}

fn add_asset_changelog_entry(
    tables: &AssetHubTables,
    txn: &mut RwTransaction,
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
    let mut value = value_builder.init_root::<asset_change_log_entry::Builder>();
    value.reborrow().set_num(last_seq);
    {
        let mut value = value.reborrow().init_event();
        match change {
            ChangeEvent::ContentUpdate(evt) => {
                let mut db_evt = value.init_content_update_event();
                db_evt.reborrow().init_id().set_id(evt.id.as_bytes());
                if let Some(ref import_hash) = evt.import_hash {
                    db_evt.reborrow().init_import_hash().set_hash(import_hash);
                }
                if let Some(ref build_dep_hash) = evt.build_dep_hash {
                    db_evt.reborrow().set_build_dep_hash(build_dep_hash);
                }
            }
            ChangeEvent::Remove(id) => {
                value.init_remove_event().init_id().set_id(id.as_bytes());
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
            listeners: Mutex::new(SlotMap::new()),
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

    pub fn get_build_deps_reverse<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        id: &AssetUUID,
    ) -> Result<Option<MessageReader<'a, data::asset_uuid_list::Owned>>> {
        Ok(txn
            .get::<data::asset_uuid_list::Owned, _>(self.tables.build_dep_reverse, id.as_bytes())?)
    }

    fn put_build_deps_reverse(
        &self,
        txn: &mut RwTransaction,
        id: &AssetUUID,
        dependees: Vec<AssetUUID>,
    ) -> Result<()> {
        let mut value_builder = capnp::message::Builder::new_default();
        let mut value = value_builder.init_root::<data::asset_uuid_list::Builder>();
        let mut list = value.reborrow().init_list(dependees.len() as u32);
        for (idx, uuid) in dependees.iter().enumerate() {
            list.reborrow().get(idx as u32).set_id(uuid.as_bytes());
        }
        txn.put(self.tables.build_dep_reverse, id.as_bytes(), &value_builder)?;
        Ok(())
    }

    pub fn update_asset(
        &self,
        txn: &mut RwTransaction,
        import_hash: u64,
        metadata: &AssetMetadata,
        source: data::AssetSource,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let hash_bytes = import_hash.to_le_bytes();
        let mut maybe_id = None;
        let existing_metadata: Option<MessageReader<imported_metadata::Owned>> =
            txn.get(self.tables.asset_metadata, metadata.id.as_bytes())?;
        let new_metadata =
            build_imported_metadata::<&[u8; 8]>(&metadata, Some(&hash_bytes), source);
        let mut deps_to_delete = Vec::new();
        let mut deps_to_add = Vec::new();
        if let Some(existing_metadata) = existing_metadata {
            let existing_metadata = existing_metadata.get()?;
            let latest_artifact = existing_metadata.get_latest_artifact();
            if let latest_artifact::Id(Ok(id)) = latest_artifact.which()? {
                maybe_id = Some(Vec::from(id.get_hash()?));
            }
            for dep in existing_metadata.get_metadata()?.get_build_deps()? {
                let dep = AssetUUID::from_slice(dep.get_id()?)?;
                if metadata.build_deps.contains(&dep) == false {
                    deps_to_delete.push(dep);
                }
                deps_to_add.remove_item(&dep);
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
                    dependees.push(AssetUUID::from_slice(uuid.get_id()?)?);
                }
            }
            dependees.push(metadata.id);
            self.put_build_deps_reverse(txn, &dep, dependees)?;
        }
        for dep in deps_to_delete {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    dependees.push(AssetUUID::from_slice(uuid.get_id()?)?);
                }
            }
            dependees.remove_item(&metadata.id);
            if dependees.is_empty() {
                txn.delete(self.tables.build_dep_reverse, &dep.as_bytes())?;
            } else {
                self.put_build_deps_reverse(txn, &dep, dependees)?;
            }
        }
        txn.put(
            self.tables.asset_metadata,
            metadata.id.as_bytes(),
            &new_metadata,
        )?;
        if artifact_changed {
            change_batch.content_changes.push(metadata.id);
        }
        Ok(())
    }

    pub fn remove_asset(
        &self,
        txn: &mut RwTransaction,
        id: &AssetUUID,
        change_batch: &mut ChangeBatch,
    ) -> Result<()> {
        let mut artifact_hash = None;
        let metadata = self.get_metadata(txn, id)?;
        let mut deps_to_delete = Vec::new();
        if let Some(metadata) = metadata {
            let metadata = metadata.get()?;
            if let latest_artifact::Id(Ok(id)) = metadata.get_latest_artifact().which()? {
                let hash = id.get_hash()?;
                if !hash.is_empty() {
                    artifact_hash = Some(Vec::from(hash));
                }
            }
            for dep in metadata.get_metadata()?.get_build_deps()? {
                deps_to_delete.push(AssetUUID::from_slice(dep.get_id()?)?);
            }
        }
        if txn.delete(self.tables.asset_metadata, id.as_bytes())? {
            change_batch.content_changes.push(*id);
        }
        for dep in deps_to_delete {
            let mut dependees = Vec::new();
            if let Some(existing_list) = self.get_build_deps_reverse(txn, &dep)? {
                for uuid in existing_list.get()?.get_list()? {
                    dependees.push(AssetUUID::from_slice(uuid.get_id()?)?);
                }
            }
            dependees.remove_item(&id);
            if dependees.is_empty() {
                txn.delete(self.tables.build_dep_reverse, &dep.as_bytes())?;
            } else {
                self.put_build_deps_reverse(txn, &dep, dependees)?;
            }
        }
        Ok(())
    }

    pub fn add_changes(&self, txn: &mut RwTransaction, change_batch: ChangeBatch) -> Result<bool> {
        // TODO find the set of all changed assets, check the build dependency index and emit changes for all
        // assets that have changed and all the assets where the build_dep_hash has changed.
        // dedupe change events
        let mut to_check = VecDeque::new();
        let mut affected_assets = HashSet::new();
        let mut events = Vec::new();
        for id in change_batch.content_changes {
            to_check.push_back(id);
        }
        if to_check.len() > 0 {
            log::info!("{} assets changed content", to_check.len());
        }
        while !to_check.is_empty() {
            let id = to_check.pop_front().unwrap();
            if affected_assets.insert(id) {
                if let Some(dependees) = self.get_build_deps_reverse(txn, &id)? {
                    for dependee in dependees.get()?.get_list()? {
                        to_check.push_back(AssetUUID::from_slice(dependee.get_id()?)?);
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
                        for dep in metadata.get_metadata()?.get_build_deps()? {
                            to_check.push_back(AssetUUID::from_slice(dep.get_id()?)?);
                        }
                    }
                }
                let mut sorted_assets: Vec<(&AssetUUID, &Vec<u8>)> =
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
                    import_hash: Some(Vec::from(import_hash)),
                    build_dep_hash: Some(Vec::from(&build_dep_hash.to_le_bytes() as &[u8])),
                }));
            } else {
                events.push(ChangeEvent::Remove(asset));
            }
        }
        if events.len() > 0 {
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
                    to_remove.push(id);
                }
                _ => {}
            }
        }
        for id in to_remove {
            listeners.remove(id);
        }
    }

    pub fn register_listener(&self, listener: Sender<AssetBatchEvent>) -> ListenerID {
        self.listeners.lock().unwrap().insert(listener)
    }

    pub fn drop_listener(&self, listener: ListenerID) -> Option<Sender<AssetBatchEvent>> {
        self.listeners.lock().unwrap().remove(listener)
    }
}
