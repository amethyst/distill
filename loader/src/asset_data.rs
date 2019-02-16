// use crate::loader::{AssetStorage, AssetType};
// use std::collections::HashMap;

// pub struct AssetDataStorage {
//     storages: HashMap<u128, Box<dyn AssetStorage>>,
// }

// impl AssetDataStorage {
//     pub fn new() -> AssetDataStorage {
//         let mut storages = HashMap::new();
//         for t in inventory::iter::<AssetType> {
//             println!("asset storage {:?}", t.uuid);
//             storages.insert(t.uuid, (t.create_storage)());
//         }
//         AssetDataStorage { storages }
//     }

//     pub fn storage<'a>(&'a self, uuid: u128) -> Option<&'a dyn AssetStorage> {
//         self.storages.get(&uuid).map(|s| s.as_ref())
//     }
// }
