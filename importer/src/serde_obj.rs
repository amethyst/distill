use downcast::*;
use erased_serde::*;
use type_uuid::TypeUuidDynamic;

/// A trait for serializing any struct with a TypeUuid
pub trait SerdeObj: Any + Serialize + TypeUuidDynamic + Send {}
impl<T: Serialize + TypeUuidDynamic + Send + 'static> SerdeObj for T {}

serialize_trait_object!(SerdeObj);
downcast!(dyn SerdeObj);
