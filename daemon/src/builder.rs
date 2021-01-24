use distill_schema::data::artifact;

pub trait BuildFunc {
    fn build(artifact: artifact::Reader);
}

pub struct BuildContext {}

pub struct Builder {
    build_funcs: HashMap<AssetTypeId, Box<dyn BuildFunc>>,
}

impl Builder {
    pub fn build(artifact: &artifact::Reader) {}
}
