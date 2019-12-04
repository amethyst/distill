# Atelier Assets
An asset management & processing framework for game engines.

# Motivation
Popular commercially available game engines struggle to provide a good user experience for game projects with a large volume of content. Game projects are continuously increasing in size with many AAA projects exceeding hundreds of gigabytes in source assets that are being concurrently produced by hundreds of content creators. The time spent by developers waiting on processing or building of content costs the industry immensely every year. This project aims to provide a collection of libraries, tools and services that can be integrated into a game engine to minimize redundant processing and scale builds to the capabilities your hardware. 

# Vision
To create a solid open-source default alternative for asset management, processing and indexing with mid-to-large sized game development teams as the primary users.

# Features
The project contains a number of different components, and some can be used independently of others. Checkmarks indicate feature support - some features are planned but not implemented.

## Daemon 
The Daemon watches for filesystem events, imports source files, manages metadata and serves asset load requests. It is primarily intended for use during development, but can also be used in a distributed game if appropriate. The Daemon uses less than 10MB RAM and only does work when either a file changes or work is requested.

<details><summary>&check; <b>Asset UUIDs & Dependency Graphs</b></summary><p>Every asset is identified by a 16-byte UUID that is generated when a source file is imported for the first time. Importers also produce an asset's build and load dependencies in terms of UUIDs which can be used to efficiently traverse the dependency graph of an asset without touching the filesystem. </p></details>
<details><summary>&check; <b>Source file change detection</b></summary><p>The daemon watches for filesystem changes and ensures source files are only imported when they change. Metadata and hashes are indexed locally in LMDB and version controlled in .meta files. Filesystem modification time and hashes are used to reduce redundant imports across your whole team to the greatest extent possible.</p></details>
<details><summary>&check; <b>Asset Change Log</b></summary><p>Asset metadata is maintained in LMDB, a transactional database. The database's consistency guarantees and snapshot support provides a way to synchronize external data stores with the current state of the asset metadata using the Asset Change Log of asset changes.</p></details>
<details><summary>&check; <b>Metadata Tracking & Caching</b></summary><p>When assets are imported from source files, metadata is generated and stored in `.meta` files together with source file, as well as cached in a database. Commit these to version control along with your source files.</p></details>
<details><summary>&check; <b>Move & Rename Source Files Confidently</b></summary><p>Since metadata is stored with the source file and UUIDs are used to identify individual assets, users can move, rename and share source files with others without breaking references between assets.</p></details>
<details><summary>&check; <b>Bring Your Own Asset Types</b></summary><p>Asset types are not included in this project. You define your own asset types and source file formats by implementing the `Importer` trait and registering these with a file extension. The Daemon will automatically run your `Importer` for files with the registered extension as required. All asset types must implement `serde::Serialize` + `serde::Deserialize` + `type_uuid::TypeUuidDynamic` + `Send`.</p></details>
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
The Loader module provides a `Loader` trait for loading assets and an `AssetStorage` trait for storing assets. Game engines usually only need to implement the `AssetStorage` trait, as an optional RpcLoader implementation that loads assets from the Daemon is provided.
<details><summary>&check; <b>Hot Reloading</b> </summary><p>The built-in `RpcLoader` talks to the `Daemon` and automatically reloads assets in the running engine when an asset has changed.</p></details>
<details><summary>&check; <b>Automatic Loading of Dependencies</b> </summary><p>When a source file is imported and an asset is produed, dependencies are gathered for the asset and saved as metadata. Loader implementations automatically ensure that dependencies are loaded before the asset is loaded, and that they are unloaded after the asset is unloaded.</p></details>
<details><summary>&check; <b>serde` Support for Handles</b> 🎉💯 </summary><p>An optional Handle type is provided with support for deserialization and serialization using `serde`. Deserialization works for both an asset's UUID and its path. The UUID or path is automatically resolved to a Handle that refers to the corresponding loaded asset.</p></details>
<details><summary>&check; <b>Automatic Registration of Handle Dependencies</b> 🎉💯</summary><p>Handle references are automatically registered during import for an asset and the referenced assets are subsequently guaranteed to be loaded by the Loader before the depender asset is loaded. This means Handles in assets are guaranteed to be valid.</p></details>


## TODO
<details><summary><b>Import Caching</b></summary><p>Assets imported from a source file can be cached by a hash of their source file content and its ID, avoiding expensive parsing and disk operations across your whole team.</p></details>
<details><summary><b>Build Artifact Caching</b></summary><p>Assets are built using the provided build parameters only when requested. An asset's build artifact can be cached by a hash of its build dependencies, build parameters and source file content.</p></details>
<details><summary><b>Networked artifact caching</b></summary><p>Results of imports and builds can be re-used across your whole team using a networked cache server.</p></details>
<details><summary><b>Platform-specific builds</b></summary><p>Provide customized build parameters when building an asset and tailor the build artifact for a specific platform.</p></details>
<details><summary><b>User-defined Asset Build Graphs</b></summary><p>Users can define execution graphs for asset pre-processing  that are run when preparing an asset for loading.</p></details>
<details><summary><b>Scalable build pipeline</b></summary><p>Once assets are imported from sources, the build system aims to be completely pure in the functional programming sense. Inputs to asset builds are all known and declared in the import step. This design enables parallelizable and even distributed builds.</p></details>
<details><summary><b>Searching</b></summary><p>Search tags can be produced at import and are automatically indexed by <a href="https://github.com/tantivy-search/tantivy">tantivy</a> which enables <a href="https://tantivy-search.github.io/bench/">super fast text search</a>. The search index is incrementally maintained by subscribing to the Asset Change Log.</p></details>
<details><summary><b>Packing for distribution</b></summary><p>To distribute your game, you will want to pack assets into files with enough metadata to load them quickly. With the asset dependency graph known, it is possible implement custom asset packing schemes by applying knowledge about your game's usage pattern to optimize for sequential access.</p></details>

# Examples
Compilation dependencies:
- [capnpc in PATH](https://capnproto.org/install.html)

To run:
- `cd examples/handle_integration`
- `cargo run`
- The example includes an image asset type, so try to put some images (png, jpg, tga) in the `assets` folder!

Have a look at the generated `.meta` files in the `assets` folder!

# Get involved
This project's first user will be - [Amethyst](https://github.com/amethyst/amethyst) and casual communication around development happens in the #assets channel of the - [Amethyst Discord server](https://discord.gg/amethyst). Feel free to drop by for a chat, contributions or questions are very welcome! 
