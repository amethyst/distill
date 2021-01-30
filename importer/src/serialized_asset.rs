use distill_core::ArtifactMetadata;

pub struct SerializedAsset<T: AsRef<[u8]>> {
    pub metadata: ArtifactMetadata,
    pub data: T,
}

impl<'a> SerializedAsset<&'a [u8]> {
    pub fn to_vec(&self) -> SerializedAsset<Vec<u8>> {
        SerializedAsset {
            metadata: self.metadata.clone(),
            data: self.data.to_vec(),
        }
    }
}
