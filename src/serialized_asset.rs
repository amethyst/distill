use crate::error::Result;
use atelier_importer::SerdeObj;
use bincode;
use schema::data::CompressionType;

pub struct SerializedAsset<T: AsRef<[u8]>> {
    pub compression: CompressionType,
    pub uncompressed_size: usize,
    pub type_uuid: uuid::Bytes,
    pub data: T,
}

impl SerializedAsset<Vec<u8>> {
    pub fn create(
        value: &dyn SerdeObj,
        compression: CompressionType,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<SerializedAsset<Vec<u8>>> {
        let size = bincode::serialized_size(value)? as usize;
        scratch_buf.clear();
        scratch_buf.resize(size, 0);
        bincode::serialize_into(scratch_buf.as_mut_slice(), value)?;
        let asset_buf = {
            use smush::{encode, Encoding, Quality};
            let quality = Quality::Maximum;
            match compression {
                CompressionType::None => scratch_buf.clone(),
                CompressionType::Lz4 => encode(scratch_buf, Encoding::Lz4, quality)?,
            }
        };

        Ok(SerializedAsset {
            compression,
            uncompressed_size: size,
            type_uuid: value.uuid(),
            data: asset_buf,
        })
    }
}
