use atelier_assets::{
    core::{type_uuid, type_uuid::TypeUuid},
    importer::{self as atelier_importer, typetag, SerdeImportable},
    loader::handle::Handle,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, TypeUuid, SerdeImportable, Debug)]
#[uuid = "fab4249b-f95d-411d-a017-7549df090a4f"]
pub struct BigPerf {
    pub cool_string: String,
    pub handle_made_from_path: Handle<crate::image::Image>,
    pub handle_made_from_uuid: Handle<crate::image::Image>,
}
