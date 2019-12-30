use crate::asset_hub::{self, AssetHub};
use crate::capnp_db::{CapnpCursor, DBTransaction, Environment, MessageReader, RwTransaction};
use crate::daemon::ImporterMap;
use crate::error::{Error, Result};
use crate::file_tracker::{FileState, FileTracker, FileTrackerEvent};
use crate::serialized_asset::SerializedAsset;
use crate::source_pair_import::{
    self, hash_file, HashedSourcePair, SourceMetadata, SourcePair, SourcePairImport,
};
use atelier_core::{utils, AssetRef, AssetUuid, CompressionType};
use atelier_importer::{ArtifactMetadata, AssetMetadata, BoxedImporter, ImporterContext};
use atelier_schema::data::{self, path_refs, source_metadata};
use bincode;
use crossbeam_channel::{self as channel, Receiver};
use log::{debug, error, info};
use rayon::prelude::*;
use scoped_threadpool::Pool;
use std::collections::{HashMap, HashSet};
use std::{path::PathBuf, str, sync::Arc, time::Instant};

pub(crate) struct FileAssetSource {
    hub: Arc<AssetHub>,
    tracker: Arc<FileTracker>,
    rx: Receiver<FileTrackerEvent>,
    db: Arc<Environment>,
    tables: FileAssetSourceTables,
    importers: Arc<ImporterMap>,
    importer_contexts: Arc<Vec<Box<dyn ImporterContext>>>,
}

struct FileAssetSourceTables {
    /// Maps the source file path to its SourceMetadata
    /// Path -> SourceMetadata
    path_to_metadata: lmdb::Database,
    /// Maps an AssetUuid to its source file path
    /// AssetUuid -> Path
    asset_id_to_path: lmdb::Database,
    /// Reverse index of a path reference to a list of paths to source files referencing the path
    /// Path -> PathRefs
    reverse_path_refs: lmdb::Database,
}

#[derive(Debug)]
struct AssetImportResultMetadata {
    pub metadata: AssetMetadata,
    pub unresolved_load_refs: Vec<AssetRef>,
    pub unresolved_build_refs: Vec<AssetRef>,
}
struct PairImportResultMetadata<'a> {
    pub import_state: SourcePairImport<'a>,
    pub assets: Vec<AssetImportResultMetadata>,
}

type SerializedAssetVec = SerializedAsset<Vec<u8>>;

fn hash_files<'a, T, I>(pairs: I) -> Vec<Result<HashedSourcePair>>
where
    I: IntoParallelIterator<Item = &'a SourcePair, Iter = T>,
    T: ParallelIterator<Item = &'a SourcePair>,
{
    Vec::from_par_iter(pairs.into_par_iter().map(|s| {
        let mut hashed_pair = HashedSourcePair {
            meta: s.meta.clone(),
            source: s.source.clone(),
            source_hash: None,
            meta_hash: None,
        };
        match s.meta {
            Some(ref state) if state.state == data::FileState::Exists => {
                let (state, hash) = hash_file(state)?;
                hashed_pair.meta = Some(state);
                hashed_pair.meta_hash = hash;
            }
            _ => {}
        };
        match s.source {
            Some(ref state) if state.state == data::FileState::Exists => {
                let (state, hash) = hash_file(state)?;
                hashed_pair.source = Some(state);
                hashed_pair.source_hash = hash;
            }
            _ => {}
        };
        Ok(hashed_pair)
    }))
}

fn resolve_source_path(abs_source_path: &PathBuf, path: &PathBuf) -> PathBuf {
    let absolute_path = if path.is_relative() {
        // TODO check from root of asset folder as well?
        let mut parent_path = abs_source_path.clone();
        parent_path.pop();
        parent_path.push(path);
        parent_path
    } else {
        path.clone()
    };
    crate::watcher::canonicalize_path(&absolute_path)
}

impl FileAssetSource {
    pub fn new(
        tracker: &Arc<FileTracker>,
        hub: &Arc<AssetHub>,
        db: &Arc<Environment>,
        importers: &Arc<ImporterMap>,
        importer_contexts: Arc<Vec<Box<dyn ImporterContext>>>,
    ) -> Result<FileAssetSource> {
        let (tx, rx) = channel::unbounded();
        tracker.register_listener(tx);
        Ok(FileAssetSource {
            tracker: tracker.clone(),
            hub: hub.clone(),
            db: db.clone(),
            rx,
            tables: FileAssetSourceTables {
                path_to_metadata: db
                    .create_db(Some("path_to_metadata"), lmdb::DatabaseFlags::default())?,
                asset_id_to_path: db
                    .create_db(Some("asset_id_to_path"), lmdb::DatabaseFlags::default())?,
                reverse_path_refs: db
                    .create_db(Some("reverse_path_refs"), lmdb::DatabaseFlags::default())?,
            },
            importers: importers.clone(),
            importer_contexts,
        })
    }

    fn put_metadata<'a>(
        &self,
        txn: &'a mut RwTransaction<'_>,
        path: &PathBuf,
        metadata: &SourceMetadata,
    ) -> Result<Vec<AssetUuid>> {
        let mut affected_assets = Vec::new();
        let (assets_to_remove, path_refs_to_remove): (Vec<AssetUuid>, Vec<PathBuf>) = self
            .get_metadata(txn, path)
            .map(|existing| {
                let existing = existing.get().expect("capnp: Failed to read metadata");
                let path_refs = existing
                    .get_path_refs()
                    .expect("capnp: Failed to get path refs")
                    .iter()
                    .map(|r| {
                        PathBuf::from(
                            str::from_utf8(r.expect("cpnp: Failed to read path ref"))
                                .expect("Failed to parse path ref as utf8"),
                        )
                    })
                    .collect();
                let assets = existing.get_assets().expect("capnp: Failed to get assets");

                let asset_ids = assets
                    .iter()
                    .map(|asset| {
                        asset
                            .get_id()
                            .and_then(|id| id.get_id())
                            .map_err(Error::Capnp)
                            .and_then(|slice| {
                                Ok(utils::uuid_from_slice(slice).ok_or(Error::UuidLength)?)
                            })
                            .expect("capnp: Failed to read uuid")
                    })
                    .filter(|id| metadata.assets.iter().all(|a| a.id != *id))
                    .collect();

                (asset_ids, path_refs)
            })
            .unwrap_or_default();

        for asset in assets_to_remove {
            debug!("removing deleted asset {:?}", asset);
            self.delete_asset_path(txn, &asset);
            affected_assets.push(asset);
        }
        for asset in metadata.assets.iter() {
            debug!("updating asset {:?}", asset.id);

            match self.get_asset_path(txn, &asset.id) {
                Some(ref old_path) if old_path != path => {
                    error!(
                        "asset {:?} already in DB with path {} expected {}",
                        asset.id,
                        old_path.to_string_lossy(),
                        path.to_string_lossy(),
                    );
                }
                Some(_) => {} // asset already in DB with correct path
                _ => self.put_asset_path(txn, &asset.id, path),
            }

            affected_assets.push(asset.id);
        }
        for path_ref in path_refs_to_remove {
            self.remove_path_ref(txn, path, &path_ref);
        }

        let new_path_refs = metadata
            .assets
            .iter()
            .filter_map(|x| x.artifact.as_ref())
            .flat_map(|x| &x.load_deps)
            .chain(
                metadata
                    .assets
                    .iter()
                    .filter_map(|x| x.artifact.as_ref())
                    .flat_map(|x| &x.build_deps),
            )
            .filter_map(|x| {
                if let AssetRef::Path(path) = x {
                    Some(path)
                } else {
                    None
                }
            });
        let mut deduped_path_refs = HashSet::new();
        for path_ref in new_path_refs {
            if deduped_path_refs.insert(path_ref.clone()) {
                self.add_path_ref(txn, path, &path_ref);
            }
        }
        let mut value_builder = capnp::message::Builder::new_default();

        {
            let mut value = value_builder.init_root::<source_metadata::Builder<'_>>();

            {
                value.set_importer_version(metadata.importer_version);
                value.set_importer_type(&metadata.importer_type.0);
                value.set_importer_state_type(&metadata.importer_state.uuid());
                let mut state_buf = Vec::new();
                bincode::serialize_into(&mut state_buf, &metadata.importer_state)?;
                value.set_importer_state(&state_buf);
                value.set_importer_options_type(&metadata.importer_options.uuid());
                let mut options_buf = Vec::new();
                bincode::serialize_into(&mut options_buf, &metadata.importer_options)?;
                value.set_importer_options(&options_buf);
                let hash_bytes = metadata
                    .import_hash
                    .expect("import hash not present")
                    .to_le_bytes();
                value.set_import_hash(&hash_bytes);
            }
            let mut path_refs = value
                .reborrow()
                .init_path_refs(deduped_path_refs.len() as u32);
            for (idx, path_ref) in deduped_path_refs.into_iter().enumerate() {
                path_refs
                    .reborrow()
                    .set(idx as u32, path_ref.to_string_lossy().as_bytes());
            }

            let mut assets = value.reborrow().init_assets(metadata.assets.len() as u32);

            for (idx, asset) in metadata.assets.iter().enumerate() {
                let mut builder = assets.reborrow().get(idx as u32);
                asset_hub::build_asset_metadata(asset, &mut builder, data::AssetSource::File);
            }

            let assets_with_pipelines: Vec<&AssetMetadata> = metadata
                .assets
                .iter()
                .filter(|a| a.build_pipeline.is_some())
                .collect();
            let mut build_pipelines = value
                .reborrow()
                .init_build_pipelines(assets_with_pipelines.len() as u32);

            for (idx, asset) in assets_with_pipelines.iter().enumerate() {
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_key()
                    .set_id(&asset.id.0);
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_value()
                    .set_id(&asset.build_pipeline.unwrap().0);
            }
        }

        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.put(self.tables.path_to_metadata, &key, &value_builder)
            .expect("db: Failed to put value to path_to_metadata");

        Ok(affected_assets)
    }

    pub fn get_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Option<MessageReader<'a, source_metadata::Owned>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        txn.get::<source_metadata::Owned, &[u8]>(self.tables.path_to_metadata, &key)
            .expect("db: Failed to get source metadata from path_to_metadata table")
    }

    pub fn iter_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
    ) -> impl Iterator<Item = (PathBuf, MessageReader<'a, source_metadata::Owned>)> {
        txn.open_ro_cursor(self.tables.path_to_metadata)
            .expect("db: Failed to open ro cursor for path_to_metadata table")
            .capnp_iter_start()
            .filter_map(|(key, value)| {
                let evt = value
                    .expect("capnp: Failed to read event")
                    .into_typed::<source_metadata::Owned>();
                let path = PathBuf::from(str::from_utf8(key).ok()?);
                Some((path, evt))
            })
    }

    fn delete_metadata(&self, txn: &mut RwTransaction<'_>, path: &PathBuf) -> Vec<AssetUuid> {
        let to_remove: Vec<AssetUuid> = self
            .get_metadata(txn, path)
            .map(|existing| {
                let metadata = existing.get().expect("capnp: Failed to read metadata");
                metadata
                    .get_assets()
                    .expect("capnp: Failed to get assets")
                    .iter()
                    .map(|asset| {
                        asset
                            .get_id()
                            .and_then(|id| id.get_id())
                            .map_err(Error::Capnp)
                            .and_then(|slice| {
                                Ok(utils::uuid_from_slice(slice).ok_or(Error::UuidLength)?)
                            })
                            .expect("capnp: Failed to read uuid")
                    })
                    .collect()
            })
            .unwrap_or_default();

        for asset in to_remove.iter() {
            debug!("remove asset {:?}", asset);
            self.delete_asset_path(txn, &asset);
        }

        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        txn.delete(self.tables.path_to_metadata, &key)
            .expect("db: Failed to delete metadata from path_to_metadata table");
        to_remove
    }

    pub fn resolve_asset_ref<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        source_path: &PathBuf,
        asset_ref: &AssetRef,
    ) -> Option<AssetUuid> {
        match asset_ref {
            AssetRef::Uuid(uuid) => Some(*uuid),
            AssetRef::Path(path) => {
                let canon_path = resolve_source_path(source_path, path);
                if let Some(metadata) = self.get_metadata(txn, &canon_path) {
                    let assets = metadata
                        .get()
                        .map_err(crate::error::Error::Capnp)
                        .and_then(|metadata| {
                            let mut assets = Vec::new();
                            for asset in metadata.get_assets()? {
                                assets.push(
                                    utils::uuid_from_slice(asset.get_id()?.get_id()?)
                                        .ok_or(Error::UuidLength)?,
                                );
                            }
                            Ok(assets)
                        })
                        .expect("capnp: failed to read asset list");
                    // Resolve the path into asset with index 0, if it exists
                    assets.into_iter().nth(0)
                } else {
                    log::error!(
                        "Failed to resolve path {:?} at {:?}: could not find metadata for file",
                        canon_path.to_string_lossy(),
                        source_path.to_string_lossy(),
                    );
                    None
                }
            }
        }
    }

    fn put_asset_path<'a>(
        &self,
        txn: &'a mut RwTransaction<'_>,
        asset_id: &AssetUuid,
        path: &PathBuf,
    ) {
        let path_str = path.to_string_lossy();
        let path = path_str.as_bytes();
        txn.put_bytes(self.tables.asset_id_to_path, asset_id, &path)
            .expect("db: Failed to put asset path to asset_id_to_path table");
    }

    pub fn get_asset_path<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        asset_id: &AssetUuid,
    ) -> Option<PathBuf> {
        txn.get_as_bytes(self.tables.asset_id_to_path, asset_id)
            .expect("db: Failed to get asset_id from asset_id_to_path table")
            .map(|p| PathBuf::from(str::from_utf8(p).expect("utf8: Failed to parse path")))
    }

    fn delete_asset_path(&self, txn: &mut RwTransaction<'_>, asset_id: &AssetUuid) -> bool {
        txn.delete(self.tables.asset_id_to_path, asset_id)
            .expect("db: Failed to delete asset_id from asset_id_to_path table")
    }

    fn add_path_ref<'a>(
        &self,
        txn: &'a mut RwTransaction<'_>,
        source: &PathBuf,
        path_ref: &PathBuf,
    ) -> bool {
        let path_ref = resolve_source_path(source, path_ref);
        let key_str = path_ref.to_string_lossy();
        let key = key_str.as_bytes();
        let existing_refs = txn
            .get::<path_refs::Owned, &[u8]>(self.tables.reverse_path_refs, &key)
            .expect("db: Failed to get path ref from reverse_path_refs table");
        let path_ref_str = source.to_string_lossy();
        let path_ref_bytes = path_ref_str.as_bytes();
        let mut message = capnp::message::Builder::new_default();
        let list = message.init_root::<path_refs::Builder<'_>>();
        let mut new_size = 1;
        let mut paths = if let Some(existing_refs) = existing_refs {
            let existing_refs = existing_refs.get().expect("capnp: failed to read message");
            let existing_refs = existing_refs
                .get_paths()
                .expect("capnp: failed to read paths");

            for existing_path in existing_refs.iter() {
                if existing_path.expect("capnp: failed to read path ref") == path_ref_bytes {
                    return false; // already exists in the list
                }
            }
            new_size += existing_refs.len();
            let mut paths = list.init_paths(new_size);
            for (idx, existing_path) in existing_refs.iter().enumerate() {
                paths.set(
                    idx as u32,
                    existing_path.expect("capnp: failed to read path ref"),
                );
            }
            paths
        } else {
            list.init_paths(1)
        };
        paths.set(new_size - 1, &path_ref_bytes);
        txn.put(self.tables.reverse_path_refs, &key, &message)
            .expect("lmdb: failed to put path ref");
        true
    }

    pub fn get_path_refs<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Vec<PathBuf> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        txn.get::<path_refs::Owned, &[u8]>(self.tables.reverse_path_refs, &key)
            .expect("db: Failed to get asset_id from asset_id_to_path table")
            .map_or(Vec::new(), |path_refs_message| {
                let path_refs_message = path_refs_message
                    .get()
                    .expect("capnp: failed to read message");
                let path_refs = path_refs_message
                    .get_paths()
                    .expect("capnp: failed to read paths");
                path_refs
                    .iter()
                    .map(|path_bytes| {
                        PathBuf::from(
                            std::str::from_utf8(
                                path_bytes.expect("capnp: failed to read path ref"),
                            )
                            .expect("capnp: failed to read utf8"),
                        )
                    })
                    .collect()
            })
    }

    fn remove_path_ref(
        &self,
        txn: &mut RwTransaction<'_>,
        source: &PathBuf,
        path_ref: &PathBuf,
    ) -> bool {
        let path_ref = resolve_source_path(source, path_ref);
        let key_str = path_ref.to_string_lossy();
        let key = key_str.as_bytes();
        let existing_refs = txn
            .get::<path_refs::Owned, &[u8]>(self.tables.reverse_path_refs, &key)
            .expect("db: Failed to get path ref from reverse_path_refs table");
        if let Some(existing_refs) = existing_refs {
            let path_ref_str = source.to_string_lossy();
            let path_ref_bytes = path_ref_str.as_bytes();
            let existing_refs = existing_refs.get().expect("capnp: failed to read message");
            let existing_refs = existing_refs
                .get_paths()
                .expect("capnp: failed to read paths");

            let mut remove_idx = None;
            for (idx, existing_path) in existing_refs.iter().enumerate() {
                if existing_path.expect("capnp: failed to read path ref") == path_ref_bytes {
                    remove_idx = Some(idx);
                }
            }
            match remove_idx {
                None => false, // does not exist in current list
                Some(remove_idx) => {
                    let new_size = existing_refs.len() - 1;
                    if new_size == 0 {
                        txn.delete(self.tables.reverse_path_refs, &key)
                            .expect("lmdb: failed to delete path ref");
                    } else {
                        let mut message = capnp::message::Builder::new_default();
                        let list = message.init_root::<path_refs::Builder<'_>>();
                        let mut paths = list.init_paths(new_size);
                        let mut insert_idx = 0;
                        for (idx, existing_path) in existing_refs.iter().enumerate() {
                            if idx != remove_idx {
                                paths.set(
                                    insert_idx as u32,
                                    existing_path.expect("capnp: failed to read path ref"),
                                );
                                insert_idx += 1;
                            }
                        }
                        txn.put(self.tables.reverse_path_refs, &key, &message)
                            .expect("db: failed to update path refs");
                    }
                    true
                }
            }
        } else {
            false
        }
    }

    pub fn regenerate_import_artifact<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        id: &AssetUuid,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<(u64, SerializedAssetVec)> {
        let path = self
            .get_asset_path(txn, id)
            .ok_or_else(|| Error::Custom("Could not find asset".to_string()))?;
        let cache = DBSourceMetadataCache {
            txn,
            file_asset_source: self,
            _marker: std::marker::PhantomData,
        };

        let mut import = SourcePairImport::new(path.clone());
        import.set_importer_from_map(&self.importers);
        import.set_importer_contexts(&self.importer_contexts);
        import.generate_source_metadata(&cache);
        import.hash_source();
        let imported_assets = import.import_source(scratch_buf)?;
        let mut context_set = imported_assets
            .importer_context_set
            .expect("importer context set required");
        let unresolved_load_refs = imported_assets
            .assets
            .iter()
            .flat_map(|a| &a.unresolved_load_refs);
        let unresolved_build_refs = imported_assets
            .assets
            .iter()
            .flat_map(|a| &a.unresolved_build_refs);
        let mut resolved_refs = Vec::new();
        for unresolved_ref in unresolved_build_refs.chain(unresolved_load_refs) {
            if let Some(uuid) = self.resolve_asset_ref(txn, &path, &unresolved_ref) {
                context_set.resolve_ref(&unresolved_ref, uuid);
                resolved_refs.push(uuid);
            }
        }

        context_set.enter();
        let serialized_asset = imported_assets
            .assets
            .into_iter()
            .find(|a| a.metadata.id == *id)
            .ok_or_else(|| Error::Custom("Asset does not exist in source file".to_string()))
            .and_then(|a| {
                let import_hash = import
                    .import_hash()
                    .expect("Invalid: Import path should exist");

                let hash = utils::calc_asset_hash(id, import_hash, resolved_refs);
                context_set.begin_serialize_asset(a.metadata.id);
                let serialized_asset = SerializedAsset::create(
                    hash,
                    *id,
                    a.metadata
                        .artifact
                        .as_ref()
                        .map(|artifact| artifact.build_deps.clone())
                        .unwrap_or(Vec::new()),
                    a.metadata
                        .artifact
                        .as_ref()
                        .map(|artifact| artifact.load_deps.clone())
                        .unwrap_or(Vec::new()),
                    &*a.asset
                        .expect("expected asset obj when regenerating artifact"),
                    CompressionType::None,
                    scratch_buf,
                )?;
                context_set.end_serialize_asset(a.metadata.id);

                Ok((hash, serialized_asset))
            });
        context_set.exit();
        serialized_asset
    }

    fn resolve_metadata_asset_refs<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
        asset_import_result: &AssetImportResultMetadata,
        artifact: &mut ArtifactMetadata,
    ) {
        for unresolved_build_ref in asset_import_result.unresolved_build_refs.iter() {
            if let Some(build_ref) = self.resolve_asset_ref(txn, path, unresolved_build_ref) {
                let uuid_ref = AssetRef::Uuid(build_ref);
                if !artifact.build_deps.contains(&uuid_ref) {
                    artifact.build_deps.push(uuid_ref);
                }
                // remove the AssetRef that was resolved
                let ref_idx = artifact
                    .build_deps
                    .iter()
                    .position(|x| x == unresolved_build_ref);
                if let Some(ref_idx) = ref_idx {
                    artifact.build_deps.remove(ref_idx);
                }
            }
        }
        for unresolved_load_ref in asset_import_result.unresolved_load_refs.iter() {
            if let Some(load_ref) = self.resolve_asset_ref(txn, path, unresolved_load_ref) {
                let uuid_ref = AssetRef::Uuid(load_ref);
                if !artifact.load_deps.contains(&uuid_ref) {
                    artifact.load_deps.push(uuid_ref);
                }
                let ref_idx = artifact
                    .load_deps
                    .iter()
                    .position(|x| x == unresolved_load_ref);
                if let Some(ref_idx) = ref_idx {
                    artifact.load_deps.remove(ref_idx);
                }
            }
        }
    }

    fn process_metadata_changes(
        &self,
        txn: &mut RwTransaction<'_>,
        changes: HashMap<PathBuf, Option<PairImportResultMetadata<'_>>>,
        change_batch: &mut asset_hub::ChangeBatch,
    ) {
        let mut affected_assets = HashMap::new();

        // delete metadata for deleted source pairs
        for (path, _) in changes.iter().filter(|(_, change)| change.is_none()) {
            debug!("deleting metadata for {}", path.to_string_lossy());
            for asset in self.delete_metadata(txn, path) {
                affected_assets.entry(asset).or_insert(None);
            }
        }

        // update or insert metadata for changed source pairs
        for (path, metadata) in changes.iter().filter(|(_, change)| change.is_some()) {
            let import_state = &metadata.as_ref().unwrap().import_state;
            if import_state.source_metadata().is_none() {
                continue;
            }
            let metadata = import_state
                .source_metadata()
                .unwrap_or_else(|| panic!("Change for {:?} has no SourceMetadata", path));
            debug!("imported {}", path.to_string_lossy());

            let changed_assets = self
                .put_metadata(txn, path, &metadata)
                .expect("Failed to put metadata");

            for asset in changed_assets {
                affected_assets.entry(asset).or_insert(None);
            }

            for asset in metadata.assets.iter() {
                affected_assets.insert(asset.id, Some(asset.clone()));
            }
        }

        // resolve unresolved path AssetRefs into UUIDs before updating asset metadata.
        for (path, metadata) in changes.iter().filter(|(_, change)| change.is_some()) {
            let metadata = metadata.as_ref().unwrap();
            for asset in metadata.assets.iter() {
                let asset_metadata = affected_assets
                    .get_mut(&asset.metadata.id)
                    .expect("asset in changes but not in affected_assets")
                    .as_mut()
                    .expect("asset None in affected_assets");
                if let Some(artifact) = asset_metadata.artifact.as_mut() {
                    self.resolve_metadata_asset_refs(txn, path, asset, artifact);
                }
            }
        }

        // push removals and updates into AssetHub database
        for (asset, maybe_metadata) in affected_assets.iter_mut() {
            match self.get_asset_path(txn, &asset) {
                Some(ref path) => {
                    let asset_metadata = maybe_metadata
                        .as_mut()
                        .expect("metadata exists in DB but not in hashmap");
                    let import_hash = changes
                        .get(path)
                        .expect("path in affected set but no change in hashmap")
                        .as_ref()
                        .expect("path changed but no import result present")
                        .import_state
                        .import_hash()
                        .expect("path changed but no import hash present");
                    // TODO set error state for unresolved path references?
                    // this code strips out path references for now, but it will probably crash and burn when loading
                    asset_metadata.artifact.as_mut().map(|a| {
                        a.load_deps = a
                            .load_deps
                            .iter()
                            .filter(|x| x.is_uuid())
                            .cloned()
                            .collect();
                        a.build_deps = a
                            .build_deps
                            .iter()
                            .filter(|x| x.is_uuid())
                            .cloned()
                            .collect();
                        a.hash = utils::calc_asset_hash(
                            &asset,
                            import_hash,
                            a.load_deps.iter().chain(a.build_deps.iter()),
                        )
                    });

                    self.hub
                        .update_asset(txn, &asset_metadata, data::AssetSource::File, change_batch)
                        .expect("hub: Failed to update asset in hub");
                }
                None => {
                    self.hub
                        .remove_asset(txn, &asset, change_batch)
                        .expect("hub: Failed to remove asset");
                }
            }
        }

        // update asset hashes for the reverse path refs of all changes
        for (path, _) in changes.iter() {
            let reverse_path_refs = self.get_path_refs(txn, path);
            for path_ref_source in reverse_path_refs.iter() {
                // First, check if the path has already been processed
                if changes.contains_key(path_ref_source) {
                    continue;
                }
                // Then we look in the database for assets affected by the change
                let cache = DBSourceMetadataCache {
                    txn,
                    file_asset_source: &self,
                    _marker: std::marker::PhantomData,
                };
                let mut import = SourcePairImport::new(path_ref_source.clone());
                if !import.set_importer_from_map(&self.importers) {
                    log::warn!("failed to set importer from map for path {:?} when updating path ref dependencies", path_ref_source);
                } else {
                    import.generate_source_metadata(&cache);
                    let import_hash = import
                        .source_metadata()
                        .expect("expected source metadata")
                        .import_hash
                        .expect("expected import hash in source metadata");
                    match import.import_result_from_metadata() {
                        Ok(import_result) => {
                            for mut asset in import_result.assets {
                                let result_metadata = AssetImportResultMetadata {
                                    metadata: asset.metadata.clone(),
                                    unresolved_load_refs: asset.unresolved_load_refs,
                                    unresolved_build_refs: asset.unresolved_build_refs,
                                };
                                if let Some(artifact) = &mut asset.metadata.artifact {
                                    self.resolve_metadata_asset_refs(
                                        txn,
                                        path_ref_source,
                                        &result_metadata,
                                        artifact,
                                    );
                                    // TODO set error state for unresolved path references?
                                    // this code strips out path references for now, but it will probably crash and burn when loading
                                    artifact.load_deps = artifact
                                        .load_deps
                                        .iter()
                                        .filter(|x| x.is_uuid())
                                        .cloned()
                                        .collect();
                                    artifact.build_deps = artifact
                                        .build_deps
                                        .iter()
                                        .filter(|x| x.is_uuid())
                                        .cloned()
                                        .collect();
                                    artifact.hash = utils::calc_asset_hash(
                                        &asset.metadata.id,
                                        import_hash,
                                        artifact.load_deps.iter().chain(artifact.build_deps.iter()),
                                    );
                                    self.hub
                                        .update_asset(
                                            txn,
                                            &asset.metadata,
                                            data::AssetSource::File,
                                            change_batch,
                                        )
                                        .expect("hub: Failed to update asset in hub");
                                }
                            }
                        }
                        Err(err) => {
                            log::error!("failed to get import result from metadata when updating path ref for asset: {}", err);
                        }
                    }
                }
            }
        }
    }

    fn ack_dirty_file_states(&self, txn: &mut RwTransaction<'_>, pair: &HashedSourcePair) {
        let mut skip_ack_dirty = false;

        {
            let check_file_state = |s: &Option<&FileState>| -> bool {
                match s {
                    Some(source) => {
                        let source_file_state = self.tracker.get_file_state(txn, &source.path);
                        source_file_state.map_or(false, |s| s != **source)
                    }
                    None => false,
                }
            };

            skip_ack_dirty |= check_file_state(&pair.source.as_ref().map(|f| f));
            skip_ack_dirty |= check_file_state(&pair.meta.as_ref().map(|f| f));
        }
        if !skip_ack_dirty {
            if pair.source.is_some() {
                self.tracker
                    .delete_dirty_file_state(txn, pair.source.as_ref().map(|p| &p.path).unwrap());
            }

            if pair.meta.is_some() {
                self.tracker
                    .delete_dirty_file_state(txn, pair.meta.as_ref().map(|p| &p.path).unwrap());
            }
        }
    }

    fn handle_rename_events(&self, txn: &mut RwTransaction<'_>) {
        let rename_events = self.tracker.read_rename_events(txn);
        debug!("rename events");

        for (_, evt) in rename_events.iter() {
            let dst_str = evt.dst.to_string_lossy();
            let dst = dst_str.as_bytes();
            let mut asset_ids = Vec::new();
            let mut existing_metadata = None;

            {
                let metadata = self.get_metadata(txn, &evt.src);
                if let Some(metadata) = metadata {
                    let metadata = metadata.get().expect("capnp: Failed to get metadata");
                    let mut copy = capnp::message::Builder::new_default();
                    copy.set_root(metadata)
                        .expect("capnp: Failed to set root for metadata");

                    existing_metadata = Some(copy);
                    for asset in metadata.get_assets().expect("capnp: Failed to get assets") {
                        let id = asset
                            .get_id()
                            .and_then(|a| a.get_id())
                            .expect("capnp: Failed to get asset id");
                        asset_ids.push(Vec::from(id));
                    }
                }
            }

            for asset in asset_ids {
                txn.delete(self.tables.asset_id_to_path, &asset)
                    .expect("db: Failed to delete from asset_id_to_path table");

                txn.put_bytes(self.tables.asset_id_to_path, &asset, &dst)
                    .expect("db: Failed to put to asset_id_to_path table");
            }

            if let Some(existing_metadata) = existing_metadata {
                self.delete_metadata(txn, &evt.src);
                txn.put(self.tables.path_to_metadata, &dst, &existing_metadata)
                    .expect("db: Failed to put to path_to_metadata table");
            }
        }

        if !rename_events.is_empty() {
            self.tracker.clear_rename_events(txn);
        }
    }

    fn check_for_importer_changes(&self) -> bool {
        let changed_paths: Vec<PathBuf> = {
            let txn = self.db.ro_txn().expect("db: Failed to open ro txn");

            self.tracker
                .read_all_files(&txn)
                .iter()
                .filter_map(|file_state| {
                    let metadata = self.get_metadata(&txn, &file_state.path);
                    let changed = self
                        .importers
                        .get_by_path(&file_state.path)
                        .map(|importer| {
                            match &metadata {
                                None => {
                                    // there's no existing import metadata, but we have an importer,
                                    // so we should process this file - it probably just got a new importer
                                    return true;
                                }
                                Some(metadata) => {
                                    let metadata =
                                        metadata.get().expect("capnp: Failed to get metadata");
                                    let importer_version = metadata.get_importer_version();

                                    let options_type = metadata
                                        .get_importer_options_type()
                                        .expect("capnp: Failed to get importer options type");

                                    let state_type = metadata
                                        .get_importer_state_type()
                                        .expect("capnp: Failed to get importer state type");

                                    let importer_type = metadata
                                        .get_importer_type()
                                        .expect("capnp: Failed to get importer type");

                                    importer_version != importer.version()
                                        || options_type != importer.default_options().uuid()
                                        || state_type != importer.default_state().uuid()
                                        || importer_type != importer.uuid()
                                }
                            }
                        })
                        .unwrap_or_else(|| {
                            if let None = metadata {
                                // there's no importer, and no existing metadata.
                                // no need to process it
                                false
                            } else {
                                // there's no importer, but we have metadata.
                                // we should process it, as its importer could've been removed
                                true
                            }
                        });

                    if changed {
                        Some(file_state.path.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        let has_changed_paths = !changed_paths.is_empty();
        if has_changed_paths {
            let mut txn = self.db.rw_txn().expect("Failed to open rw txn");
            changed_paths.iter().for_each(|p| {
                self.tracker
                    .add_dirty_file(&mut txn, &p)
                    .unwrap_or_else(|err| error!("Failed to add dirty file, {}", err));
            });
            txn.commit().expect("Failed to commit txn");
        }

        has_changed_paths
    }

    fn handle_dirty_files(&self, txn: &mut RwTransaction<'_>) -> HashMap<PathBuf, SourcePair> {
        let dirty_files = self.tracker.read_dirty_files(txn);
        let mut source_meta_pairs: HashMap<PathBuf, SourcePair> = HashMap::new();

        if !dirty_files.is_empty() {
            for state in dirty_files.into_iter() {
                let mut is_meta = false;
                if let Some(ext) = state.path.extension() {
                    if let Some("meta") = ext.to_str() {
                        is_meta = true;
                    }
                }
                let base_path = if is_meta {
                    state.path.with_file_name(state.path.file_stem().unwrap())
                } else {
                    state.path.clone()
                };
                let mut pair = source_meta_pairs.entry(base_path).or_insert(SourcePair {
                    source: Option::None,
                    meta: Option::None,
                });
                if is_meta {
                    pair.meta = Some(state.clone());
                } else {
                    pair.source = Some(state.clone());
                }
            }

            for (path, pair) in source_meta_pairs.iter_mut() {
                if pair.meta.is_none() {
                    let path = utils::to_meta_path(&path);
                    pair.meta = self.tracker.get_file_state(txn, &path);
                }

                if pair.source.is_none() {
                    pair.source = self.tracker.get_file_state(txn, &path);
                }
            }

            debug!("Processing {} changed file pairs", source_meta_pairs.len());
        }

        source_meta_pairs
    }

    // TODO(happens): Return for this is asset_metadata_changed. This function needs a lot
    // of work, and in the process it will hopefully clear up and get a name that will
    // make the return value more obvious.
    fn process_asset_metadata(
        &self,
        thread_pool: &mut Pool,
        txn: &mut RwTransaction<'_>,
        hashed_files: &[HashedSourcePair],
    ) -> bool {
        use std::cell::RefCell;
        thread_local!(static SCRATCH_STORE: RefCell<Option<Vec<u8>>> = RefCell::new(None));

        let mut asset_metadata_changed = false;

        // Should get rid of this scoped_threadpool madness somehow,
        // but can't use par_iter directly since I need to process the results
        // as soon as they are completed. So essentially I want futures::stream::FuturesUnordered.
        // But I couldn't figure all that future stuff out, so here we are. Scoped threadpool.
        // TODO(happens): Handle errors inside of this scope
        thread_pool
            .scoped(|scope| -> Result<()> {
                let (tx, rx) = channel::unbounded();
                let to_process = hashed_files.len();
                let mut import_iter = hashed_files.iter().map(|p| {
                    let processed_pair = p.clone();
                    let sender = tx.clone();
                    scope.execute(move || {
                        SCRATCH_STORE.with(|cell| {
                            let mut local_store = cell.borrow_mut();
                            if local_store.is_none() {
                                *local_store = Some(Vec::new());
                            }
                            match self.db.ro_txn() {
                                Err(e) => {
                                    panic!("failed to open RO transaction: {}", e);
                                }
                                Ok(read_txn) => {
                                    let cache = DBSourceMetadataCache {
                                        txn: &read_txn,
                                        file_asset_source: &self,
                                        _marker: std::marker::PhantomData,
                                    };
                                    let result = source_pair_import::process_pair(
                                        &cache,
                                        &self.importers,
                                        &self.importer_contexts,
                                        &processed_pair,
                                        local_store.as_mut().unwrap(),
                                    );
                                    // TODO put import artifact in cache if it doesn't have unresolved refs
                                    let import_result = result.map(|result| {
                                        result.and_then(|(import, import_output)| {
                                            import_output.map(|o| PairImportResultMetadata {
                                                import_state: import,
                                                assets: o
                                                    .assets
                                                    .into_iter()
                                                    .map(|a| AssetImportResultMetadata {
                                                        metadata: a.metadata,
                                                        unresolved_load_refs: a
                                                            .unresolved_load_refs,
                                                        unresolved_build_refs: a
                                                            .unresolved_build_refs,
                                                    })
                                                    .collect(),
                                            })
                                        })
                                    });
                                    sender.send((processed_pair, import_result)).unwrap();
                                }
                            }
                        });
                    });
                });

                let num_queued_imports = num_cpus::get() * 2;
                for _ in 0..num_queued_imports {
                    import_iter.next();
                }

                let mut num_processed = 0;
                let mut metadata_changes = HashMap::new();
                while num_processed < to_process {
                    match rx.recv() {
                        Ok((pair, maybe_result)) => {
                            match maybe_result {
                                // Successful import
                                Ok(result) => {
                                    let path = &pair
                                    .source
                                    .as_ref()
                                    .or_else(|| pair.meta.as_ref())
                                    .expect(
                                        "a successful import must have a source or meta FileState",
                                    )
                                    .path;
                                    self.ack_dirty_file_states(txn, &pair);
                                    metadata_changes.insert(path.clone(), result);
                                }
                                Err(e) => error!(
                                    "Error processing pair at {:?}: {}",
                                    pair.source.as_ref().map(|s| &s.path),
                                    e
                                ),
                            }
                            num_processed += 1;
                            import_iter.next();
                        }
                        _ => {
                            break;
                        }
                    }
                }
                let mut change_batch = asset_hub::ChangeBatch::new();
                self.process_metadata_changes(txn, metadata_changes, &mut change_batch);
                asset_metadata_changed = self.hub.add_changes(txn, change_batch)?;
                Ok(())
            })
            .expect("threadpool: Failed to process metadata changes");

        asset_metadata_changed
    }

    fn handle_update(&self, thread_pool: &mut Pool) {
        let start_time = Instant::now();
        let mut changed_files = Vec::new();

        // Transactions on the same thread cannot be active at the same time!
        // This scope is important, even if it may look like it does nothing..
        {
            let mut txn = self.db.rw_txn().expect("Failed to open rw txn");

            // Before reading the filesystem state we need to process rename events.
            // This must be done in the same transaction to guarantee database consistency.
            self.handle_rename_events(&mut txn);
            let source_meta_pairs = self.handle_dirty_files(&mut txn);

            // This looks a little stupid, since there is no `into_values`
            changed_files.extend(source_meta_pairs.into_iter().map(|(_, v)| v));

            if txn.dirty {
                txn.commit().expect("Failed to commit txn");
            }
        }

        let hashed_files = hash_files(&changed_files);
        debug!("Hashed {}", hashed_files.len());

        let hashed_files: Vec<HashedSourcePair> = hashed_files
            .into_iter()
            .filter_map(|f| match f {
                Ok(hashed_file) => Some(hashed_file),
                Err(err) => {
                    error!("Hashing error: {}", err);
                    None
                }
            })
            .collect();

        let elapsed = Instant::now().duration_since(start_time);
        debug!(
            "Hashed {} pairs in {}",
            hashed_files.len(),
            elapsed.as_secs_f32()
        );

        let mut txn = self.db.rw_txn().expect("Failed to open rw txn");
        let asset_metadata_changed =
            self.process_asset_metadata(thread_pool, &mut txn, &hashed_files);

        if txn.dirty {
            txn.commit().expect("Failed to commit txn");

            if asset_metadata_changed {
                self.hub.notify_listeners();
            }
        }

        let elapsed = Instant::now().duration_since(start_time);
        info!(
            "Processed {} pairs in {}",
            hashed_files.len(),
            elapsed.as_secs_f32()
        );
    }

    pub fn run(&self) {
        let mut thread_pool = Pool::new(num_cpus::get() as u32);
        let mut started = false;
        let mut update = false;

        while let Ok(evt) = self.rx.recv() {
            match evt {
                FileTrackerEvent::Start => {
                    started = true;
                    if self.check_for_importer_changes() || update {
                        self.handle_update(&mut thread_pool);
                    }
                }
                FileTrackerEvent::Update => {
                    update = true;
                    if started {
                        self.handle_update(&mut thread_pool);
                    }
                }
            }
        }
    }
}

struct DBSourceMetadataCache<'a, 'b, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a> {
    txn: &'a V,
    file_asset_source: &'b FileAssetSource,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, 'b, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>
    source_pair_import::SourceMetadataCache for DBSourceMetadataCache<'a, 'b, V, T>
{
    fn restore_metadata(
        &self,
        path: &PathBuf,
        importer: &dyn BoxedImporter,
        metadata: &mut SourceMetadata,
    ) -> Result<()> {
        let saved_metadata = self.file_asset_source.get_metadata(self.txn, path);
        if let Some(saved_metadata) = saved_metadata {
            let saved_metadata = saved_metadata.get()?;
            let mut build_pipelines = HashMap::new();
            for pair in saved_metadata.get_build_pipelines()?.iter() {
                build_pipelines.insert(
                    utils::uuid_from_slice(&pair.get_key()?.get_id()?).ok_or(Error::UuidLength)?,
                    utils::uuid_from_slice(&pair.get_value()?.get_id()?)
                        .ok_or(Error::UuidLength)?,
                );
            }
            if saved_metadata.get_importer_options_type()? == metadata.importer_options.uuid() {
                if let Ok(options) =
                    importer.deserialize_options(saved_metadata.get_importer_options()?)
                {
                    metadata.importer_options = options;
                }
            }
            if saved_metadata.get_importer_state_type()? == metadata.importer_state.uuid() {
                if let Ok(state) = importer.deserialize_state(saved_metadata.get_importer_state()?)
                {
                    metadata.importer_state = state;
                }
            }
            metadata.import_hash = Some(u64::from_le_bytes(utils::make_array(
                saved_metadata.get_import_hash()?,
            )));
            metadata.assets = saved_metadata
                .get_assets()?
                .iter()
                .map(|a| asset_hub::parse_db_metadata(&a))
                .collect();
        }
        Ok(())
    }
}
