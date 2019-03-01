use crate::error::Result;
use atelier_importer::SerdeObj;
use schema::data::{CompressionType};
use uuid::Uuid;
use bincode;

pub struct SerializedAsset<T: AsRef<[u8]>> {
  pub compression: CompressionType,
  pub uncompressed_size: usize,
  pub type_uuid: Uuid,
  pub data: T,
}

impl SerializedAsset<Vec<u8>> {
    pub fn create(value: &dyn SerdeObj, compression: CompressionType, scratch_buf: &mut Vec<u8>) -> Result<SerializedAsset<Vec<u8>>> {
        let size = bincode::serialized_size(value)? as usize;
        scratch_buf.clear();
        scratch_buf.resize(size, 0);
        bincode::serialize_into(scratch_buf.as_mut_slice(), value)?;
        let asset_buf = {
            use smush::{Encoding, Quality, encode};
            let quality = Quality::Maximum;
            match compression {
                CompressionType::None => scratch_buf.clone(),
                CompressionType::Lz4 => encode(scratch_buf, Encoding::Lz4, quality)?,
            }
        };

        Ok(SerializedAsset {
            compression,
            uncompressed_size: size,
            type_uuid: Uuid::from_bytes(value.uuid().to_le_bytes()),
            data: asset_buf,
        })
    }
}
