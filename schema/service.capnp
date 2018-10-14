@0x805eb2f9d3deb354;

struct FileState {
    path @0 :Text;
    state @1 :import "data.capnp".FileState;
}


interface FileTracker {
    registerListener @0 (listener :Listener);
    getSnapshot @1 () -> (snapshot :Snapshot);

    interface Snapshot {
        getAllFiles @0 () -> (files :List(FileState));
        getType @1 (path :Text) -> (type :import "data.capnp".FileType);
    }

    interface Listener {
        # Called with the initial consistent state of the filesystem
        sync @0 (files :List(FileState), snapshot :Snapshot);
        # Called when a batch of file updates has been processed
        fileUpdate @1 (updatedFiles :List(FileState), snapshot :Snapshot);
    }
}
