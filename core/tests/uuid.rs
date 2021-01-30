extern crate bincode;
extern crate distill_core;
extern crate serde_json;

#[test]
fn serialize_asset_uuid_string() {
    let uuid = distill_core::AssetUuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    let result = serde_json::to_string(&uuid).unwrap();

    assert_eq!(
        "\"01020304-0506-0708-090a-0b0c0d0e0f10\"".to_string(),
        result
    );
}

#[test]
fn serialize_asset_uuid_binary() {
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let uuid = distill_core::AssetUuid(data);

    let result: Vec<u8> = bincode::serialize(&uuid).unwrap();

    assert_eq!(data.to_vec(), result);
}

#[test]
fn deserialize_asset_uuid_string() {
    let string = "\"01020304-0506-0708-090a-0b0c0d0e0f10\"";

    let result: distill_core::AssetUuid = serde_json::from_str(string).unwrap();

    let expected = distill_core::AssetUuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    assert_eq!(expected, result);
}

#[test]
fn deserialize_asset_uuid_binary() {
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let result: distill_core::AssetUuid = bincode::deserialize(&data).unwrap();

    assert_eq!(distill_core::AssetUuid(data), result);
}

#[test]
fn serialize_type_uuid_string() {
    let uuid = distill_core::AssetTypeId([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3]);

    let result = serde_json::to_string(&uuid).unwrap();

    assert_eq!(
        "\"03010401-0509-0206-0503-050809070903\"".to_string(),
        result
    );
}

#[test]
fn serialize_type_uuid_binary() {
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let uuid = distill_core::AssetTypeId(data);

    let result: Vec<u8> = bincode::serialize(&uuid).unwrap();

    assert_eq!(data.to_vec(), result);
}

#[test]
fn deserialize_type_uuid_string() {
    let string = "\"03010401-0509-0206-0503-050809070903\"";

    let result: distill_core::AssetTypeId = serde_json::from_str(string).unwrap();

    let expected = distill_core::AssetTypeId([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3]);

    assert_eq!(expected, result);
}

#[test]
fn deserialize_type_uuid_binary() {
    let data = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3];

    let result: distill_core::AssetTypeId = bincode::deserialize(&data).unwrap();

    assert_eq!(distill_core::AssetTypeId(data), result);
}
