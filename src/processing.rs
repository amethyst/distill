use asset_import::SerdeObj;

enum Type {
    List(Box<Type>),
    Struct(TypeUUID),
}

pub trait ProcessingNode {
    fn inputs() -> Vec<Type>; 
    fn outputs() -> Vec<Type>; 
    fn run(Vec<Box<SerdeObj>>) -> Vec<Box<SerdeObj>>
}