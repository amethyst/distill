use atelier_importer::SerdeImportable;
use atelier_loader::handle::Handle;
use serde::{Deserialize, Serialize};
use type_uuid::TypeUuid;

#[derive(Serialize, Deserialize, TypeUuid, SerdeImportable, Debug)]
#[uuid = "fab4249b-f95d-411d-a017-7549df090a4f"]
pub struct BigPerf {
    pub cool_string: String,
    pub but_cooler_handle: Handle<crate::image::Image>,
}
