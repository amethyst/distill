use crate::{error::Result, ArtifactMetadata, SerdeObj};
use atelier_core::{AssetRef, AssetTypeId, AssetUuid, CompressionType};
use bincode;

pub struct SerializedAsset<T: AsRef<[u8]>> {
    pub metadata: ArtifactMetadata,
    pub data: T,
}

impl SerializedAsset<Vec<u8>> {
    pub fn create(
        hash: u64,
        id: AssetUuid,
        build_deps: Vec<AssetRef>,
        load_deps: Vec<AssetRef>,
        value: &dyn SerdeObj,
        compression: CompressionType,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<SerializedAsset<Vec<u8>>> {
        let size = bincode::serialized_size(value)? as usize;
        scratch_buf.clear();
        scratch_buf.resize(size, 0);
        bincode::serialize_into(scratch_buf.as_mut_slice(), value)?;
        let asset_buf = {
            #[cfg(feature = "compression")]
            use smush::{encode, Encoding, Quality};
            #[cfg(feature = "compression")]
            let quality = Quality::Maximum;
            match compression {
                CompressionType::None => scratch_buf.clone(),
                #[cfg(feature = "compression")]
                CompressionType::Lz4 => encode(scratch_buf, Encoding::Lz4, quality)?,
                #[cfg(not(feature = "compression"))]
                CompressionType::Lz4 => {
                    panic!("compression not enabled: compile with `compression` feature")
                }
            }
        };

        Ok(SerializedAsset {
            metadata: ArtifactMetadata {
                id,
                hash,
                build_deps,
                load_deps,
                compression,
                uncompressed_size: Some(size as u64),
                compressed_size: Some(asset_buf.len() as u64),
                type_id: AssetTypeId(value.uuid()),
            },
            data: asset_buf,
        })
    }
}

impl<'a> SerializedAsset<&'a [u8]> {
    pub fn to_vec(&self) -> SerializedAsset<Vec<u8>> {
        SerializedAsset {
            metadata: self.metadata.clone(),
            data: self.data.to_vec(),
        }
    }
}
