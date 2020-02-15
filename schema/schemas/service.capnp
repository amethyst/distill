@0x805eb2f9d3deb354;

using D = import "data.capnp";
struct AssetPath {
    id @0 :D.AssetUuid;
    path @1 :Data;
}
struct PathAssets {
    path @0 :Data;
    assets @1 :List(D.AssetUuid);
}
struct AssetData {
    data @0 :Data;
    typeId @1 :Data;
}
interface AssetHub {
    registerListener @0 (listener :Listener) -> ();
    getSnapshot @1 () -> (snapshot :Snapshot);

    interface Snapshot {
        getAssetMetadata @0 (assets :List(D.AssetUuid)) -> (assets :List(D.AssetMetadata));
        getAssetMetadataWithDependencies @1 (assets :List(D.AssetUuid)) -> (assets :List(D.AssetMetadata));
        getAllAssetMetadata @2 () -> (assets :List(D.AssetMetadata));
        getLatestAssetChange @3 () -> (num :UInt64);
        getAssetChanges @4 (start :UInt64, count :UInt64) -> (changes :List(D.AssetChangeLogEntry));
        getImportArtifacts @5 (assets :List(D.AssetUuid)) -> (artifacts :List(D.Artifact));
        updateAsset @6 (asset :D.Artifact) -> (newImportHash :Data);
        patchAsset @7 (assetId :D.AssetUuid, assetHash :Data, patch :AssetData) -> (newImportHash :Data);

        # these are FileAssetSource specific and should probably be moved to another RPC interface
        getPathForAssets @8 (assets :List(D.AssetUuid)) -> (paths :List(AssetPath));
        getAssetsForPaths @9 (paths :List(Data)) -> (assets :List(PathAssets));
        createFile @10 (path :Data, assets :List(AssetData)) -> (newImportHash :Data);
        deleteFile @11 (path :Data) -> ();
    }

    interface Listener {
        # Called on registration and when a batch of asset updates have been processed
        update @0 (latestChange :UInt64, snapshot :Snapshot);
    }
}
