[![Rust](https://github.com/amethyst/distill/workflows/CI/badge.svg)](https://github.com/amethyst/distill/actions)

# Distill
Distill is an asset pipeline for games, reading artist-friendly formats from disk, processing them into your engine-ready formats, and delivering them to your game runtime. Distill handles dependencies between assets, import & build caching, cross-device hot reloading during development, packing assets for a shippable game build, and more.

# Vision
To create an open-source go-to solution for asset processing in games.

# Features
The project contains a number of different components, and some can be used independently of others. You can combine them in different ways to tailor them to your workflow. Checkmarks indicate feature support - some features are dreamed up but not implemented.

## Daemon 
The daemon watches for filesystem events, imports source files to produce assets, manages metadata and serves asset load requests. It is primarily intended for use during development, but can also be used in a distributed game if appropriate. The daemon is very resource efficient and only does work when either a file changes or work is requested. Other components interact with the daemon through a transport-agnostic RPC protocol.

<details><summary>&check; <b>Asset UUIDs & Dependency Graphs</b></summary><p>Every asset is identified by a 16-byte UUID that is generated when a source file is imported for the first time. Importers also produce an asset's build and load dependencies in terms of UUIDs which can be used to efficiently traverse the dependency graph of an asset without touching the filesystem. </p></details>
<details><summary>&check; <b>Source file change detection</b></summary><p>The daemon watches for filesystem changes and ensures source files are only imported when they change. Metadata and hashes are indexed locally in LMDB and version controlled in .meta files. Filesystem modification time and hashes are used to reduce redundant imports across your whole team to the greatest extent possible.</p></details>
<details><summary>&check; <b>Import Caching</b></summary><p>Assets imported from a source file are cached by a hash of their source file content and its ID, avoiding expensive parsing and disk operations.</p></details>
<details><summary>&check; <b>Asset Change Log</b></summary><p>Asset metadata is maintained in LMDB, a transactional database. The database's consistency guarantees and snapshot support provides a way to synchronize external data stores with the current state of the asset metadata using the Asset Change Log of asset changes.</p></details>
<details><summary>&check; <b>Metadata Tracking & Caching</b></summary><p>When assets are imported from source files, metadata is generated and stored in `.meta` files together with source file, as well as cached in a database. Commit these to version control along with your source files.</p></details>
<details><summary>&check; <b>Move & Rename Source Files Confidently</b></summary><p>Since metadata is stored with the source file and UUIDs are used to identify individual assets, users can move, rename and share source files with others without breaking references between assets.</p></details>
<details><summary>&check; <b>Bring Your Own Asset Types</b></summary><p>Asset types are not included in this project. You define your own asset types and source file formats by implementing the `Importer` trait and registering these with a file extension. The Daemon will automatically run your `Importer` for files with the registered extension as required. All asset types must implement `serde::Serialize` + `serde::Deserialize` + `TypeUuidDynamic` + `Send`.</p></details>
<details><summary>&check; <b>RON Importer</b> - *OPTIONAL*</summary><p>An optional Importer and derive macro is included to simplify usage of serialized Rust types as source files using `serde`.

Type definition:
```rust
#[derive(Serialize, Deserialize, TypeUuid, SerdeImportable)]
#[uuid = "fab4249b-f95d-411d-a017-7549df090a4f"]
pub struct CustomAsset {
    pub cool_string: String,
    pub handle_from_path: Handle<crate::image::Image>,
    pub handle_from_uuid: Handle<crate::image::Image>,
}
```
`custom_asset.ron`:
```
{
    "fab4249b-f95d-411d-a017-7549df090a4f": 
    (
        cool_string: "thanks",
        // This references an asset from a file in the same directory called "amethyst.png"
        handle_from_path: "amethyst.png", 
        // This references an asset with a UUID (see associated .meta file for an asset's UUID)
        handle_from_uuid: "6c5ae1ad-ae30-471b-985b-7d017265f19f"
    )
}
```


</p></details>



## Loader
The Loader module loads assets and their dependencies for a user-implemented `AssetStorage` trait to handle. Loader supports a pluggable `LoaderIO` trait for customizing where assets and their metadata are loaded from.
<details><summary>&check; <b>Hot Reloading</b> </summary><p>The built-in `RpcIO` implementation of `LoaderIO` talks to the `Daemon` and automatically reloads assets when an asset has changed.</p></details>
<details><summary>&check; <b>Automatic Loading of Dependencies</b> </summary><p>When a source file is imported and an asset is produced, dependencies are gathered for the asset and saved as metadata. The Loader automatically ensures that dependencies are loaded before the asset is loaded, and that dependencies are unloaded when they are no longer needed.</p></details>
<details><summary>&check; <b>serde` Support for Handles</b> 🎉💯 </summary><p>An optional Handle type is provided with support for deserialization and serialization using `serde`. Handles can be deserialized as either a UUID or a path.</p></details>
<details><summary>&check; <b>Automatic Registration of Handle Dependencies</b> 🎉💯</summary><p>Handle references that are serialized as part of an asset are automatically registered and the referenced assets are guaranteed to be loaded by the Loader before the depending asset is loaded. This means Handles in assets are always guaranteed to be valid and loaded.</p></details>
<details><summary><b>Packing for distribution</b></summary><p>To distribute your game, you will want to pack assets into files with enough metadata to load them quickly. The CLI supports packing assets into a file format which the `PackfileIO` implementation supports loading.</p></details>


## TODO
<details><summary><b>Networked artifact caching</b></summary><p>Results of imports and builds can be re-used across your whole team using a networked cache server.</p></details>
<details><summary><b>Platform-specific builds</b></summary><p>Provide customized build parameters when building an asset and tailor the build artifact for a specific platform.</p></details>
<details><summary><b>Scalable build pipeline</b></summary><p>Once assets are imported from sources, the build system aims to be completely pure in the functional programming sense. Inputs to asset builds are all known and declared in the import step. This design enables parallelizable and even distributed builds.</p></details>
<details><summary><b>Searching</b></summary><p>Search tags can be produced at import and are automatically indexed by <a href="https://github.com/tantivy-search/tantivy">tantivy</a> which enables <a href="https://tantivy-search.github.io/bench/">super fast text search</a>. The search index is incrementally maintained by subscribing to the Asset Change Log.</p></details>

# Cross-Platform Support
The project aims to support as many platforms as possible with the `Loader` module, while the `Daemon` may never be able to run on platforms without solid filesystem support such as WASM.
Current known supported platforms:
Linux/Mac/Windows: Loader + Daemon
iOS: Loader

# Examples
To run:
- `cd examples/handle_integration`
- `cargo run`
- The example includes an image asset type, so try to put some images (png, jpg, tga) in the `assets` folder!

Have a look at the generated `.meta` files in the `assets` folder!

# Get involved
This project is primarily used by [Amethyst](https://github.com/amethyst/amethyst) and casual communication around development happens in the #engine-general channel of the [Amethyst Discord server](https://discord.gg/amethyst). Feel free to drop by for a chat. Contributions or questions are very welcome! 

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT).

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

PLEASE NOTE that some dependencies may be licensed under other terms. These are listed in [deny.toml](deny.toml) under licenses.exceptions on a best-effort basis, and are validated in every CI run using [cargo-deny](https://github.com/EmbarkStudios/cargo-deny).
