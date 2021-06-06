use std::{
    collections::{HashMap, HashSet},
    fs::File,
    mem::ManuallyDrop,
    sync::Arc,
};

use capnp::serialize::SliceSegments;
use distill_core::{utils::make_array, AssetMetadata, AssetRef, AssetUuid};
use distill_schema::pack::pack_file;
use memmap::{Mmap, MmapOptions};
use thread_local::ThreadLocal;

use crate::{
    io::{DataRequest, LoaderIO, MetadataRequest, MetadataRequestResult, ResolveRequest},
    loader::LoaderState,
};

trait PackfileMessageReader: Send + Sync {
    fn get_reader(&self) -> capnp::Result<pack_file::Reader<'_>>;
}

/// This reader memmaps a file
struct PackfileMessageReaderFile {
    file: ManuallyDrop<File>,
    mmap: ManuallyDrop<Mmap>,
    message_reader: ManuallyDrop<ThreadLocal<capnp::message::Reader<SliceSegments<'static>>>>,
}

impl PackfileMessageReaderFile {
    pub fn new(file: File) -> std::io::Result<Self> {
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Ok(PackfileMessageReaderFile {
            file: ManuallyDrop::new(file),
            mmap: ManuallyDrop::new(mmap),
            message_reader: ManuallyDrop::new(ThreadLocal::new()),
        })
    }
}

impl PackfileMessageReader for PackfileMessageReaderFile {
    fn get_reader(&self) -> capnp::Result<pack_file::Reader<'_>> {
        let message_reader = self.message_reader.get_or_try(|| {
            let slice: &[u8] = &self.mmap;
            let mut slice: &[u8] = unsafe {
                // SAFETY: We ensure that the reader is dropped before the mmap so it's ok to cast to 'static here
                std::mem::transmute::<&[u8], &'static [u8]>(slice)
            };
            capnp_reader_from_slice(&mut slice)
        })?;
        message_reader.get_root::<pack_file::Reader<'_>>()
    }
}

impl Drop for PackfileMessageReaderFile {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.message_reader);
            ManuallyDrop::drop(&mut self.mmap);
            ManuallyDrop::drop(&mut self.file);
        }
    }
}

/// This reader loads from a buffer of bytes. Works great with a buffer returned by
/// include_bytes!(...).
///
/// The buffer must be 8-byte aligned, or the unaligned feature in the capnp crate must be enabled
struct PackfileMessageReaderBuffer {
    data: &'static [u8],
    message_reader: ManuallyDrop<ThreadLocal<capnp::message::Reader<SliceSegments<'static>>>>,
}

impl PackfileMessageReaderBuffer {
    pub fn new(data: &'static [u8]) -> std::io::Result<Self> {
        Ok(PackfileMessageReaderBuffer {
            data,
            message_reader: ManuallyDrop::new(ThreadLocal::new()),
        })
    }
}

impl PackfileMessageReader for PackfileMessageReaderBuffer {
    fn get_reader(&self) -> capnp::Result<pack_file::Reader<'_>> {
        let message_reader = self.message_reader.get_or_try(|| {
            let mut slice = self.data;
            capnp_reader_from_slice(&mut slice)
        })?;
        message_reader.get_root::<pack_file::Reader<'_>>()
    }
}

impl Drop for PackfileMessageReaderBuffer {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.message_reader);
        }
    }
}

/// Common setup for the capnp message Reader, used by `PackfileMessageReaderFile` and
/// `PackfileMessageReaderBuffer`
fn capnp_reader_from_slice<'a>(
    mut slice: &mut &'a [u8],
) -> Result<capnp::message::Reader<SliceSegments<'a>>, capnp::Error> {
    let mut options = capnp::message::ReaderOptions::new();
    options.traversal_limit_in_words(Some(1 << 31));
    capnp::serialize::read_message_from_flat_slice(&mut slice, options)
}

struct PackfileReaderInner {
    reader: Box<dyn PackfileMessageReader>,
    index_by_uuid: HashMap<AssetUuid, u32>,
    assets_by_path: HashMap<String, Vec<u32>>,
    runtime: tokio::runtime::Runtime,
}
pub struct PackfileReader(Arc<PackfileReaderInner>);

impl PackfileReader {
    pub fn new_from_file(file: File) -> capnp::Result<Self> {
        let message_reader = PackfileMessageReaderFile::new(file)?;
        Self::create(Box::new(message_reader))
    }

    pub fn new_from_buffer(buffer: &'static [u8]) -> capnp::Result<Self> {
        let message_reader = PackfileMessageReaderBuffer::new(buffer)?;
        Self::create(Box::new(message_reader))
    }

    fn create(message_reader: Box<dyn PackfileMessageReader>) -> capnp::Result<Self> {
        let reader = message_reader.get_reader()?;
        let mut index_by_uuid = HashMap::new();
        let mut assets_by_path: HashMap<String, Vec<u32>> = HashMap::new();
        for (idx, entry) in reader.get_entries()?.iter().enumerate() {
            let asset_metadata = entry.get_asset_metadata()?;
            let id = AssetUuid(make_array(asset_metadata.get_id()?.get_id()?));
            index_by_uuid.insert(id, idx as u32);
            let path = entry.get_path()?;
            let path = std::str::from_utf8(&path)?;
            assets_by_path
                .entry(path.into())
                .and_modify(|v| v.push(idx as u32))
                .or_insert_with(|| vec![idx as u32]);
        }

        Ok(PackfileReader(Arc::new(PackfileReaderInner {
            reader: message_reader,
            index_by_uuid,
            assets_by_path,
            runtime: tokio::runtime::Builder::new_multi_thread().build()?,
        })))
    }
}

impl PackfileReaderInner {
    fn get_asset_metadata_with_dependencies_impl(
        &self,
        request: &MetadataRequest,
    ) -> capnp::Result<Vec<MetadataRequestResult>> {
        let reader = self.reader.get_reader()?;
        let mut to_visit = request.requested_assets().cloned().collect::<Vec<_>>();
        let mut visited: HashSet<AssetUuid, std::collections::hash_map::RandomState> =
            to_visit.iter().cloned().collect();
        let entries = reader.get_entries()?;
        let mut metadata = Vec::new();
        while let Some(uuid) = to_visit.pop() {
            if let Some(idx) = self.index_by_uuid.get(&uuid) {
                let entry = entries.get(*idx);
                let artifact_metadata =
                    distill_schema::parse_artifact_metadata(&entry.get_artifact()?.get_metadata()?);
                for dep in &artifact_metadata.load_deps {
                    if let AssetRef::Uuid(dep_uuid) = dep {
                        if !visited.contains(&dep_uuid) {
                            visited.insert(*dep_uuid);
                            to_visit.push(*dep_uuid);
                        }
                    }
                }
                let mut result = MetadataRequestResult {
                    artifact_metadata,
                    asset_metadata: None,
                };
                if request.include_asset_metadata() {
                    result.asset_metadata = Some(distill_schema::parse_db_metadata(
                        &entry.get_asset_metadata()?,
                    ));
                }
                metadata.push(result);
            }
        }
        Ok(metadata)
    }

    fn get_artifact_impl(&self, request: &DataRequest) -> capnp::Result<Vec<u8>> {
        let reader = self.reader.get_reader()?;
        let entries = reader.get_entries()?;
        if let Some(idx) = self.index_by_uuid.get(&request.asset_id) {
            let entry = entries.get(*idx);
            Ok(Vec::from(entry.get_artifact()?.get_data()?))
        } else {
            Err(capnp::Error::failed(format!(
                "UUID {:?} not found in packfile",
                request.asset_id
            )))
        }
    }

    fn get_asset_candidates_impl(
        &self,
        request: &ResolveRequest,
    ) -> capnp::Result<Vec<(std::path::PathBuf, Vec<AssetMetadata>)>> {
        let reader = self.reader.get_reader()?;
        let entries = reader.get_entries()?;
        if let Some(indices) = self.assets_by_path.get(request.identifier().path()) {
            let mut metadata = Vec::with_capacity(indices.len());
            // TODO canonicalize the requested path
            let path = std::path::PathBuf::from(request.identifier().path().replace("\\", "/"));
            for idx in indices {
                let entry = entries.get(*idx);
                let asset_metadata =
                    distill_schema::parse_db_metadata(&entry.get_asset_metadata()?);
                metadata.push(asset_metadata);
            }
            Ok(vec![(path, metadata)])
        } else {
            Err(capnp::Error::failed(format!(
                "Identifier {:?} not found in packfile",
                request.identifier()
            )))
        }
    }
}

impl LoaderIO for PackfileReader {
    fn get_asset_metadata_with_dependencies(&mut self, request: MetadataRequest) {
        let _guard = self.0.runtime.enter();
        let inner = self.0.clone();
        tokio::spawn(async move {
            match inner.get_asset_metadata_with_dependencies_impl(&request) {
                Ok(data) => request.complete(data),
                Err(err) => request.error(err),
            }
        });
    }

    fn get_asset_candidates(&mut self, requests: Vec<ResolveRequest>) {
        let _guard = self.0.runtime.enter();
        for request in requests {
            let inner = self.0.clone();
            tokio::spawn(async move {
                match inner.get_asset_candidates_impl(&request) {
                    Ok(data) => request.complete(data),
                    Err(err) => request.error(err),
                }
            });
        }
    }

    fn get_artifacts(&mut self, requests: Vec<DataRequest>) {
        let _guard = self.0.runtime.enter();
        for request in requests {
            let inner = self.0.clone();
            tokio::spawn(async move {
                match inner.get_artifact_impl(&request) {
                    Ok(data) => request.complete(data),
                    Err(err) => request.error(err),
                }
            });
        }
    }

    fn tick(&mut self, _loader: &mut LoaderState) {}

    fn with_runtime(&self, f: &mut dyn FnMut(&tokio::runtime::Runtime)) {
        f(&self.0.runtime);
    }
}
