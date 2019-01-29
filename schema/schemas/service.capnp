@0x805eb2f9d3deb354;

using Data = import "data.capnp";

enum GetMetadataOptions {
    none @0;
    includeDependencies @1;
}
interface AssetHub {
    registerListener @0 (listener :Listener);
    getSnapshot @1 () -> (snapshot :Snapshot);

    interface Snapshot {
        getAssetMetadata @0 (assets :List(Data.AssetUuid)) -> (assets :List(Data.AssetMetadata));
        getAssetMetadataWithDependencies @1 (assets :List(Data.AssetUuid)) -> (assets :List(Data.AssetMetadata));
        getAllAssetMetadata @2 () -> (assets :List(Data.AssetMetadata));
        getBuildArtifacts @3 (assets :List(Data.AssetUuid), parameters :Data.BuildParameters) -> (artifacts :List(Data.BuildArtifact));
    }

    interface Listener {
        # Called with the initial consistent state 
        sync @0 (snapshot :Snapshot);
        # Called when a batch of asset updates have been processed
        update @1 (updated :List(Data.AssetMetadata), snapshot :Snapshot);
    }
}
