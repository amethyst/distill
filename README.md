Compilation dependencies:
- [capnpc in PATH](https://capnproto.org/install.html)

To run:

- Create an `assets` folder in the root of the repository
- Put some images (png, jpg, tga) in the `assets` folder
- `cargo run --release`

Enjoy glorious .meta files!

## Modules

### DirWatcher
Provides filesystem events with cross-platform support using the glorious crate [notify](https://docs.rs/notify/4.0.6/notify/). DirWatcher follows symlinks and supports watching multiple directories, providing events through a crossbeam_channel.

### capnp_db
Layers capnproto serialization on top of LMDB for zero-copy reads and a nicer API for using capnproto messages as keys and/or values.

### FileTracker
Receives filesystem events from DirWatcher and indexes the last seen filesystem state in a DB to provide a consistent view. Can provide real change events based on filesystem modification time and length even if a change was performed when the daemon was not active. It also maintains a set of "dirty files" to be consumed by FileAssetSource to ensure that changes always can be processed, even in the case of a crash.

### AssetHub
Indexes asset metadata by AssetID.

### FileAssetSource
Receives change events from FileTracker and performs processing on changed pairs.
1. Reads the dirty_files set from FileTracker
2. Fixes up Path->\[AssetID\] DB index based on renames
3. Creates a source file:meta pairing for each dirty file
4. Hashes the source and meta file pairs
5. "Imports" the pair: deserializes the meta file, then runs stuff in asset_import based on file extension. This produces a list of assets from the source file.
6. Indexes Path->\[AssetID\] and reverse
7. Tells AssetHub about any updated or removed assets

### AssetHubService
Provides interactive snapshots and transactions for metadata in the various modules over `capnproto-rpc`.
