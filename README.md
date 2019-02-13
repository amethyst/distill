# Atelier Assets
A "compiler framework for data" aimed at efficiently processing assets for game engines. _This project is still in an early phase and does not necessarily work as advertised. DO NOT USE._

# Motivation
Popular commercially available game engines struggle to provide a good user experience for game projects with a large volume of content. Game projects are continuously increasing in size with many AAA projects exceeding hundreds of gigabytes in source assets that are being concurrently produced by hundreds of content creators. The time spent by developers waiting on processing or building of content costs the industry immensely every year. This project aims to provide a collection of libraries, tools and services that can be integrated into a game engine to minimize redundant processing and scale builds to the capabilities your hardware. 

# Features
The project contains a number of components that can be used to achieve the following features,
- **Source file change detection**: A background process ensures source files are only parsed when they change. Metadata and hashes are indexed locally in LMDB and version controlled in .meta files, reducing redundant imports across your whole team.
- **Import caching**: Assets imported from a source file can be cached by a hash of their source file content and its ID, avoiding expensive parsing and disk operations.
- **Asset Change Log**: Asset metadata is maintained in LMDB, a transactional database. A consistent snapshot of ordered asset changes provides a way to synchronize external data stores with the current state of the asset metadata.
- **Lazy builds & caching**: Assets are built on-demand with a set of build parameters and an asset's build artifact can be cached by a hash of its build dependencies, build parameters and source file content.
- **Platform-specific builds**: Provide customized build parameters when building an asset and tailor the build artifact for a specific platform. _Not implemented yet_
- **Scalable build pipeline**: Once assets are imported from sources, the build system aims to be completely pure in the functional programming sense. Inputs to asset builds are all known and declared in the import step. This enables parallelizable and even distributed builds. _Not implemented yet_
- **Networked artifact caching**: Results of imports and builds can be re-used across your whole team using a networked cache server. _Not implemented yet_
- **Hot Reloading**: By subscribing to the Asset Change Log and fetching changed metadata, hot reloading can be optimally implemented by comparing asset hashes to determine if an asset needs to be reloaded without polling or redundant hashing. 
- **Asset dependency graphs**: Every asset is identified by a 16-byte UUID generated at source file import. Importers also produce an asset's build and load dependencies in terms of UUIDs which can be used to efficiently traverse the dependency graph of an asset without touching the filesystem. 
- **Move & Rename source files**: Since metadata is stored with the source file and UUIDs are used to identify individual assets, users can move, rename and share source files without breaking references between assets.
- **Searching**: Search tags can be produced at import and are automatically indexed by [Tantivy](https://github.com/tantivy-search/tantivy) which enables [super fast text search](https://tantivy-search.github.io/bench/). The search index is incrementally maintained by subscribing to the Asset Change Log. _Not implemented yet_
- **Packing for distribution**: To distribute your game, you will want to package assets into files with enough metadata to load them quickly. With the asset dependency graph known, it is possible implement custom asset packing schemes by applying knowledge about your game's usage pattern to optimize for sequential access. _Not implemented yet_


# Technical foundation
[LMDB](http://www.lmdb.tech/doc/) is the storage engine used for metadata indexing and artifact caching. It is [very fast](http://lmdb.tech/bench/microbench/) and especially optimized for reads. LMDB databases are memory mapped and thus support reading directly from disk pages, offering unparalleled read latency and throughput, requiring no intermediate caches. It will be possible to safely support serving cached artifacts directly from LMDB pages using `sendfile`-like APIs, most likely saturating any I/O hardware you throw at it. It also requires no background compaction or similar write amplification.
_Windows sparse file patches are used for LMDB, allowing the DB to start small and grow as needed_

[Capnproto-rust](https://github.com/capnproto/capnproto-rust) is used as the serialization format for persisted data and the RPC library for communication between the game engine and asset services. Capnproto requires no deserialization step when reading data which enables very low latency round-trips when reading from LMDB. The RPC library supports stateful object references over the network which is useful for exposing LMDB read transactions over the network, providing a consistent view of metadata even in the face of concurrent changes.

[Tantivy](https://github.com/tantivy-search/tantivy) is used for textual indexing and search. It seems to be [faster than Lucene](https://github.com/tantivy-search/tantivy) and memory maps its index, providing low memory usage.

[Notify](https://github.com/passcod/notify) provides cross-platform filesystem notifications without polling for Linux, macOS and Windows.

# Try it out
Compilation dependencies:
- [capnpc in PATH](https://capnproto.org/install.html)

To run:
- Create an `assets` folder in the root of the repository
- Put some images (png, jpg, tga) in the `assets` folder
- `cargo run --release`

Enjoy glorious .meta files!

Check the metadata using the CLI:
- `cd cli`
- Run the shell: `cargo run`
- `help` to list all available commands. Try `show_all` to get UUIDs of all indexed assets

# Get involved
This project's first user will be [Amethyst](https://github.com/amethyst/amethyst) and casual communication around development happens in the #assets channel of the [Amethyst Discord server](https://discord.gg/amethyst). Feel free to drop by for a chat, contributions or questions are very welcome! 
