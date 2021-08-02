#![deny(
    rust_2018_compatibility,
    rust_2018_idioms,
    unused,
    unused_extern_crates,
    future_incompatible,
    nonstandard_style
)]

mod schemas;
use std::path::PathBuf;

use distill_core::{utils::make_array, ArtifactId, ArtifactMetadata, AssetMetadata, AssetRef};
pub use schemas::{data_capnp, pack_capnp, service_capnp};
impl ::std::fmt::Debug for data_capnp::FileState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            data::FileState::Exists => {
                write!(f, "FileState::Exists")?;
            }
            data::FileState::Deleted => {
                write!(f, "FileState::Deleted")?;
            }
        }
        Ok(())
    }
}

impl From<distill_core::CompressionType> for data_capnp::CompressionType {
    fn from(c: distill_core::CompressionType) -> Self {
        match c {
            distill_core::CompressionType::None => Self::None,
            distill_core::CompressionType::Lz4 => Self::Lz4,
        }
    }
}

impl From<data_capnp::CompressionType> for distill_core::CompressionType {
    fn from(c: data_capnp::CompressionType) -> Self {
        match c {
            data_capnp::CompressionType::None => Self::None,
            data_capnp::CompressionType::Lz4 => Self::Lz4,
        }
    }
}

pub use crate::{data_capnp as data, pack_capnp as pack, service_capnp as service};

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

pub fn parse_db_asset_ref(asset_ref: &data::asset_ref::Reader<'_>) -> AssetRef {
    match asset_ref.which().expect("capnp: failed to read asset ref") {
        data::asset_ref::Path(p) => AssetRef::Path(PathBuf::from(
            std::str::from_utf8(p.expect("capnp: failed to read asset ref path"))
                .expect("capnp: failed to parse utf8 string"),
        )),
        data::asset_ref::Uuid(uuid) => AssetRef::Uuid(make_array(
            uuid.and_then(|id| id.get_id())
                .expect("capnp: failed to read asset ref uuid"),
        )),
    }
}

pub fn parse_artifact_metadata(artifact: &data::artifact_metadata::Reader<'_>) -> ArtifactMetadata {
    let asset_id = make_array(
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
        asset_id,
        id: ArtifactId(u64::from_le_bytes(make_array(
            artifact.get_hash().expect("capnp: failed to read hash"),
        ))),
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
        type_id: make_array(
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

pub fn parse_db_metadata(metadata: &data::asset_metadata::Reader<'_>) -> AssetMetadata {
    let asset_id = make_array(
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
    let artifact_metadata = if let data::asset_metadata::latest_artifact::Artifact(Ok(artifact)) =
        metadata
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
        Some(make_array(build_pipeline))
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
pub fn build_artifact_metadata(
    artifact_metadata: &ArtifactMetadata,
    artifact: &mut data::artifact_metadata::Builder<'_>,
) {
    let mut artifact = artifact.reborrow();
    artifact
        .reborrow()
        .init_asset_id()
        .set_id(&artifact_metadata.asset_id.0);
    artifact
        .reborrow()
        .set_hash(&artifact_metadata.id.0.to_le_bytes());
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

pub fn build_asset_metadata(
    metadata: &AssetMetadata,
    m: &mut data::asset_metadata::Builder<'_>,
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
        build_artifact_metadata(artifact_metadata, &mut artifact);
    } else {
        m.reborrow().init_latest_artifact().set_none(());
    }
    m.reborrow().set_source(source);
}

pub fn build_asset_metadata_message<K>(
    metadata: &AssetMetadata,
    source: data::AssetSource,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<data::asset_metadata::Builder<'_>>();
        build_asset_metadata(metadata, &mut m, source);
    }
    value_builder
}
