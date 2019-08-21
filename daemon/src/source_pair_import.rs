use crate::daemon::ImporterMap;
use crate::error::{Error, Result};
use crate::file_tracker::FileState;
use crate::serialized_asset::SerializedAsset;
use crate::watcher::file_metadata;
use atelier_core::utils;
use atelier_importer::{
    AssetMetadata, BoxedImporter, ImporterContext, SerdeObj,
    SourceMetadata as ImporterSourceMetadata, SOURCEMETADATA_VERSION,
};
use atelier_schema::data::{self, CompressionType};
use bincode;
use log::{debug, error, info};
use ron;
use std::{
    fs,
    hash::{Hash, Hasher},
    io::{BufRead, Read, Write},
    path::PathBuf,
};
use time::PreciseTime;

pub type SourceMetadata = ImporterSourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>;

// Only files get Some(hash)
#[derive(Clone, Debug)]
pub(crate) struct HashedSourcePair {
    pub source: Option<FileState>,
    pub source_hash: Option<u64>,
    pub meta: Option<FileState>,
    pub meta_hash: Option<u64>,
}
#[derive(Clone, Debug)]
pub(crate) struct SourcePair {
    pub source: Option<FileState>,
    pub meta: Option<FileState>,
}

pub(crate) type ImportedAssetVec = ImportedAsset<Vec<u8>>;

pub(crate) struct ImportedAsset<T: AsRef<[u8]>> {
    pub asset_hash: u64,
    pub metadata: AssetMetadata,
    pub asset: Option<SerializedAsset<T>>,
}

#[derive(Default)]
pub(crate) struct SourcePairImport<'a> {
    source: PathBuf,
    importer: Option<&'a dyn BoxedImporter>,
    importer_contexts: Option<&'a Vec<Box<dyn ImporterContext>>>,
    source_hash: Option<u64>,
    meta_hash: Option<u64>,
    import_hash: Option<u64>,
    source_metadata: Option<SourceMetadata>,
}

pub(crate) trait SourceMetadataCache {
    fn restore_metadata<'a>(
        &self,
        path: &PathBuf,
        importer: &'a dyn BoxedImporter,
        metadata: &mut SourceMetadata,
    ) -> Result<()>;
}

impl<'a> SourcePairImport<'a> {
    pub fn new(source: PathBuf) -> SourcePairImport<'a> {
        SourcePairImport {
            source,
            ..Default::default()
        }
    }
    pub fn source_metadata(self) -> Option<SourceMetadata> {
        self.source_metadata
    }
    pub fn set_source_hash(&mut self, source_hash: u64) {
        self.source_hash = Some(source_hash);
    }
    pub fn set_meta_hash(&mut self, meta_hash: u64) {
        self.meta_hash = Some(meta_hash);
    }

    pub fn hash_source(&mut self) {
        let state = FileState {
            path: self.source.clone(),
            state: data::FileState::Exists,
            last_modified: 0,
            length: 0,
        };

        hash_file(&state)
            .map(|(_, hash)| self.source_hash = hash)
            .unwrap_or_else(|err| error!("Failed to hash file: {}", err));
    }

    /// Returns true if an appropriate importer was found, otherwise false.
    pub fn set_importer_from_map(&mut self, importers: &'a ImporterMap) -> bool {
        self.importer = importers.get_by_path(&self.source);
        self.importer.is_some()
    }

    pub fn set_importer_contexts(&mut self, importer_contexts: &'a Vec<Box<dyn ImporterContext>>) {
        self.importer_contexts = Some(importer_contexts);
    }

    pub fn needs_source_import(&mut self, scratch_buf: &mut Vec<u8>) -> Result<bool> {
        if let Some(ref metadata) = self.source_metadata {
            if metadata.import_hash.is_none() {
                return Ok(true);
            }
            if self.import_hash.is_none() {
                self.import_hash = Some(self.calc_import_hash(
                    metadata.importer_options.as_ref(),
                    metadata.importer_state.as_ref(),
                    metadata.importer_version,
                    metadata.importer_type,
                    scratch_buf,
                )?);
            }
            Ok(self.import_hash.unwrap() != metadata.import_hash.unwrap())
        } else {
            Ok(true)
        }
    }

    fn calc_import_hash(
        &self,
        options: &dyn SerdeObj,
        state: &dyn SerdeObj,
        importer_version: u32,
        importer_type: uuid::Bytes,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<u64> {
        let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
        scratch_buf.clear();
        bincode::serialize_into(&mut *scratch_buf, &options)?;
        scratch_buf.hash(&mut hasher);
        scratch_buf.clear();
        bincode::serialize_into(&mut *scratch_buf, &state)?;
        scratch_buf.hash(&mut hasher);
        self.source_hash
            .expect("cannot calculate import hash without source hash")
            .hash(&mut hasher);
        importer_version.hash(&mut hasher);
        importer_type.hash(&mut hasher);
        Ok(hasher.finish())
    }

    pub fn import_hash(&self) -> Option<u64> {
        self.import_hash
    }

    pub fn read_metadata_from_file(&mut self, scratch_buf: &mut Vec<u8>) -> Result<()> {
        let importer = self
            .importer
            .expect("cannot read metadata without an importer");
        let meta = utils::to_meta_path(&self.source);
        let mut f = fs::File::open(meta)?;
        scratch_buf.clear();
        f.read_to_end(scratch_buf)?;
        self.source_metadata = Some(importer.deserialize_metadata(scratch_buf)?);
        Ok(())
    }

    pub fn generate_source_metadata<C: SourceMetadataCache>(&mut self, metadata_cache: &C) {
        let importer = self
            .importer
            // TODO(happens): Do we need to handle this?
            .expect("cannot create metadata without an importer");

        let mut default_metadata = SourceMetadata {
            version: SOURCEMETADATA_VERSION,
            importer_version: importer.version(),
            importer_type: importer.uuid(),
            importer_options: importer.default_options(),
            importer_state: importer.default_state(),
            import_hash: None,
            assets: Vec::new(),
        };

        let restored =
            metadata_cache.restore_metadata(&self.source, importer, &mut default_metadata);
        if let Ok(_) = restored {
            self.source_metadata = Some(default_metadata);
        }
    }

    fn enter_importer_contexts<F, R>(
        import_contexts: Option<&Vec<Box<dyn ImporterContext>>>,
        f: F,
    ) -> R
    where
        F: FnOnce() -> R,
    {
        let mut ctx_handles = Vec::new();
        if let Some(contexts) = import_contexts {
            for ctx in contexts.iter() {
                ctx_handles.push(ctx.enter());
            }
        }
        let retval = f();
        // make sure to exit in reverse order of enter
        for mut ctx_handle in ctx_handles.into_iter().rev() {
            ctx_handle.exit();
        }
        retval
    }

    pub fn import_source(&mut self, scratch_buf: &mut Vec<u8>) -> Result<Vec<ImportedAssetVec>> {
        let start_time = PreciseTime::now();
        let importer = self
            .importer
            .expect("cannot import source without importer");

        let metadata = std::mem::replace(&mut self.source_metadata, None)
            .expect("cannot import source file without source_metadata");
        Self::enter_importer_contexts(self.importer_contexts, || {
            let imported = {
                let mut f = fs::File::open(&self.source)?;
                importer.import_boxed(&mut f, metadata.importer_options, metadata.importer_state)?
            };
            let options = imported.options;
            let state = imported.state;
            let imported = imported.value;
            let mut imported_assets = Vec::new();
            let import_hash = self.calc_import_hash(
                options.as_ref(),
                state.as_ref(),
                importer.version(),
                importer.uuid(),
                scratch_buf,
            )?;
            self.import_hash = Some(import_hash);
            for mut asset in imported.assets {
                asset.search_tags.push((
                    "file_name".to_string(),
                    Some(
                        self.source
                            .file_name()
                            .expect("failed to get file stem")
                            .to_string_lossy()
                            .to_string(),
                    ),
                ));
                let asset_data = &asset.asset_data;
                let serialized_asset = SerializedAsset::create(
                    asset_data.as_ref(),
                    CompressionType::None,
                    scratch_buf,
                )?;
                let build_pipeline = metadata
                    .assets
                    .iter()
                    .find(|a| a.id == asset.id)
                    .and_then(|m| m.build_pipeline);
                imported_assets.push({
                    ImportedAsset {
                        asset_hash: utils::calc_asset_hash(&asset.id, import_hash),
                        metadata: AssetMetadata {
                            id: asset.id,
                            search_tags: asset.search_tags,
                            build_deps: asset.build_deps,
                            load_deps: asset.load_deps,
                            instantiate_deps: asset.instantiate_deps,
                            build_pipeline,
                            import_asset_type: asset_data.uuid(),
                        },
                        asset: Some(serialized_asset),
                    }
                });
                debug!(
                    "Import success {} read {} bytes",
                    self.source.to_string_lossy(),
                    scratch_buf.len(),
                );
            }
            info!("Imported pair in {}", start_time.to(PreciseTime::now()));
            self.source_metadata = Some(SourceMetadata {
                version: SOURCEMETADATA_VERSION,
                import_hash: Some(import_hash),
                importer_version: importer.version(),
                importer_type: importer.uuid(),
                importer_options: options,
                importer_state: state,
                assets: imported_assets.iter().map(|m| m.metadata.clone()).collect(),
            });
            Ok(imported_assets)
        })
    }

    pub fn write_metadata(&self) -> Result<()> {
        let serialized_metadata = ron::ser::to_string_pretty(
            self.source_metadata
                .as_ref()
                .expect("source_metadata missing"),
            ron::ser::PrettyConfig::default(),
        )
        .unwrap();
        let meta_path = utils::to_meta_path(&self.source);
        let mut meta_file = fs::File::create(meta_path)?;
        meta_file.write_all(serialized_metadata.as_bytes())?;
        Ok(())
    }
}

pub(crate) fn process_pair<'a, C: SourceMetadataCache>(
    metadata_cache: &C,
    importer_map: &'a ImporterMap,
    importer_contexts: &'a Vec<Box<dyn ImporterContext>>,
    pair: &HashedSourcePair,
    scratch_buf: &mut Vec<u8>,
) -> Result<Option<(SourcePairImport<'a>, Option<Vec<ImportedAssetVec>>)>> {
    let original_pair = pair.clone();
    let mut pair = pair.clone();
    // When source or meta gets deleted, the FileState has a `state` of `Deleted`.
    // For the following pattern matching, we don't want to care about the distinction between this and absence of a file.
    if let HashedSourcePair {
        source:
            Some(FileState {
                state: data::FileState::Deleted,
                ..
            }),
        ..
    } = pair
    {
        pair.source = None;
    }
    if let HashedSourcePair {
        meta:
            Some(FileState {
                state: data::FileState::Deleted,
                ..
            }),
        ..
    } = pair
    {
        pair.meta = None;
    }

    match pair {
        // Source file has been deleted
        HashedSourcePair {
            meta: None,
            source: None,
            ..
        } => {
            if let HashedSourcePair {
                source: Some(state),
                ..
            } = original_pair
            {
                debug!("deleted pair {}", state.path.to_string_lossy());
            } else if let HashedSourcePair {
                meta: Some(state), ..
            } = original_pair
            {
                debug!("deleted pair {}", state.path.to_string_lossy());
            }
            Ok(None)
        }
        // Source file with metadata
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: Some(meta_hash),
            source: Some(source),
            source_hash: Some(source_hash),
        } => {
            debug!("full pair {}", source.path.to_string_lossy());
            let mut import = SourcePairImport::new(source.path);
            import.set_source_hash(source_hash);
            import.set_meta_hash(meta_hash);
            import.set_importer_contexts(importer_contexts);
            if !import.set_importer_from_map(&importer_map) {
                Ok(None)
            } else {
                import.read_metadata_from_file(scratch_buf)?;
                if import.needs_source_import(scratch_buf)? {
                    debug!("needs source import {:?}", import.source);
                    let imported_assets = import.import_source(scratch_buf)?;
                    import.write_metadata()?;
                    Ok(Some((import, Some(imported_assets))))
                } else {
                    debug!("does not need source import {:?}", import.source);
                    Ok(Some((import, None)))
                }
            }
        }
        // Source file with no metadata
        HashedSourcePair {
            meta: None,
            source: Some(source),
            source_hash: Some(hash),
            ..
        } => {
            debug!("file without meta {}", source.path.to_string_lossy());
            let mut import = SourcePairImport::new(source.path);
            import.set_source_hash(hash);
            import.set_importer_contexts(importer_contexts);
            if !import.set_importer_from_map(&importer_map) {
                debug!("file has no importer registered");
                Ok(Some((import, None)))
            } else {
                import.generate_source_metadata(metadata_cache);
                if import.needs_source_import(scratch_buf)? {
                    let imported_assets = import.import_source(scratch_buf)?;
                    import.write_metadata()?;
                    Ok(Some((import, Some(imported_assets))))
                } else {
                    Ok(Some((import, None)))
                }
            }
        }
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: Some(_hash),
            source: Some(source),
            source_hash: None,
        } => {
            debug!("directory {}", source.path.to_string_lossy());
            Ok(None)
        }
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: None,
            source: Some(source),
            source_hash: None,
        } => {
            debug!(
                "directory with meta directory?? {}",
                source.path.to_string_lossy()
            );
            Ok(None)
        }
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: None,
            source: Some(source),
            source_hash: Some(_hash),
        } => {
            debug!(
                "source file with meta directory?? {}",
                source.path.to_string_lossy()
            );
            Ok(None)
        }
        HashedSourcePair {
            meta: None,
            source: Some(source),
            source_hash: None,
            ..
        } => {
            debug!("directory with no meta {}", source.path.to_string_lossy());
            Ok(None)
        }
        HashedSourcePair {
            meta: Some(meta),
            meta_hash: Some(_meta_hash),
            source: None,
            ..
        } => {
            debug!(
                "meta file without source file {}",
                meta.path.to_string_lossy()
            );
            fs::remove_file(&meta.path)?;
            Ok(None)
        }
        _ => {
            debug!("Unknown case for {:?}", pair);
            Ok(None)
        }
    }
}

pub(crate) fn hash_file(state: &FileState) -> Result<(FileState, Option<u64>)> {
    let metadata = match fs::metadata(&state.path) {
        Err(e) => return Err(Error::IO(e)),
        Ok(m) => {
            if !m.is_file() {
                return Ok((state.clone(), None));
            }
            file_metadata(&m)
        }
    };
    Ok(fs::OpenOptions::new()
        .read(true)
        .open(&state.path)
        .and_then(|f| {
            let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
            let mut reader = ::std::io::BufReader::with_capacity(64000, f);
            loop {
                let length = {
                    let buffer = reader.fill_buf()?;
                    hasher.write(buffer);
                    buffer.len()
                };
                if length == 0 {
                    break;
                }
                reader.consume(length);
            }
            Ok((
                FileState {
                    path: state.path.clone(),
                    state: data::FileState::Exists,
                    last_modified: metadata.last_modified,
                    length: metadata.length,
                },
                Some(hasher.finish()),
            ))
        })
        .map_err(Error::IO)?)
}
