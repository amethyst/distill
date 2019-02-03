@0x805eb2f9d3deb354;

using Data = import "data.capnp";

interface AssetHub {
    registerListener @0 (listener :Listener) -> ();
    getSnapshot @1 () -> (snapshot :Snapshot);

    interface Snapshot {
        getAssetMetadata @0 (assets :List(Data.AssetUuid)) -> (assets :List(Data.AssetMetadata));
        getAssetMetadataWithDependencies @1 (assets :List(Data.AssetUuid)) -> (assets :List(Data.AssetMetadata));
        getAllAssetMetadata @2 () -> (assets :List(Data.AssetMetadata));
        getBuildArtifacts @3 (assets :List(Data.AssetUuid), parameters :Data.BuildParameters) -> (artifacts :List(Data.BuildArtifact));
        getLatestAssetChange @4 () -> (num :UInt64);
        getAssetChanges @5 (start :UInt64, count :UInt64) -> (changes :List(Data.AssetChangeLogEntry));
    }

    interface Listener {
        # Called on registration and when a batch of asset updates have been processed
        update @0 (latestChange :UInt64, snapshot :Snapshot);
    }
}
