@0x805eb2f9d3deb354;

struct AssetState {
}


interface AssetHub {
    registerListener @0 (listener :Listener);
    getSnapshot @1 () -> (snapshot :Snapshot);

    interface Snapshot {
        getAllAssets @0 () -> (files :List(AssetState));
    }

    interface Listener {
        # Called with the initial consistent state of the filesystem
        sync @0 (snapshot :Snapshot);
        # Called when a batch of file updates has been processed
        update @1 (updated :List(AssetState), snapshot :Snapshot);
    }
}
