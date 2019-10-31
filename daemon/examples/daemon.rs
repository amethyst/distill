use std::{
    io::Read,
    net::{AddrParseError, SocketAddr},
    path::PathBuf,
};

use atelier_core::AssetUuid;
use atelier_daemon::{init_logging, AssetDaemon};
use atelier_importer::{Error, ImportedAsset, Importer, ImporterValue, Result};
use image2::{color, ImageBuf};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use type_uuid::*;

#[derive(TypeUuid, Serialize, Deserialize)]
#[uuid = "d4079e74-3ec9-4ebc-9b77-a87cafdfdada"]
enum Image {
    Rgb8(ImageBuf<u8, color::Rgb>),
    // ...
}
#[derive(TypeUuid, Serialize, Deserialize, Default)]
#[uuid = "3c8367c8-45fb-40bb-a229-00e5e9c3fc70"]
struct SimpleState(Option<AssetUuid>);
#[derive(TypeUuid)]
#[uuid = "720d636b-b79c-42d4-8f46-a2d8e1ada46e"]
struct ImageImporter;
impl Importer for ImageImporter {
    fn version_static() -> u32
    where
        Self: Sized,
    {
        1
    }
    fn version(&self) -> u32 {
        Self::version_static()
    }

    type Options = ();

    type State = SimpleState;

    /// Reads the given bytes and produces assets.
    fn import(
        &self,
        source: &mut dyn Read,
        _options: Self::Options,
        state: &mut Self::State,
    ) -> Result<ImporterValue> {
        let id = state
            .0
            .unwrap_or_else(|| AssetUuid(*uuid::Uuid::new_v4().as_bytes()));
        *state = SimpleState(Some(id));
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes)?;
        let asset = Image::Rgb8(image2::io::decode(&bytes).map_err(|e| Error::Boxed(Box::new(e)))?);
        Ok(ImporterValue {
            assets: vec![ImportedAsset {
                id,
                search_tags: vec![],
                build_deps: vec![],
                load_deps: vec![],
                build_pipeline: None,
                asset_data: Box::new(asset),
            }],
        })
    }
}
// make a macro to reduce duplication here :)
inventory::submit!(atelier_importer::SourceFileImporter {
    extension: "png",
    instantiator: || Box::new(ImageImporter {}),
});
inventory::submit!(atelier_importer::SourceFileImporter {
    extension: "jpg",
    instantiator: || Box::new(ImageImporter {}),
});
inventory::submit!(atelier_importer::SourceFileImporter {
    extension: "tga",
    instantiator: || Box::new(ImageImporter {}),
});
/// Parameters to the asset daemon.
///
/// # Examples
///
/// ```bash
/// asset_daemon --db .assets_db --address "127.0.0.1:9999" assets
/// ```
#[derive(StructOpt)]
pub struct AssetDaemonOpt {
    /// Path to the asset metadata database directory.
    #[structopt(name = "db", long, parse(from_os_str), default_value = ".assets_db")]
    pub db_dir: PathBuf,
    /// Socket address for the daemon to listen for connections, e.g. "127.0.0.1:9999".
    #[structopt(
        short,
        long,
        parse(try_from_str = "parse_socket_addr"),
        default_value = "127.0.0.1:9999"
    )]
    pub address: SocketAddr,
    /// Directories to watch for assets.
    #[structopt(parse(from_os_str), default_value = "assets")]
    pub asset_dirs: Vec<PathBuf>,
}

/// Parses a string as a socket address.
fn parse_socket_addr(s: &str) -> std::result::Result<SocketAddr, AddrParseError> {
    s.parse()
}

// This is required because rustc does not recognize .ctor segments when considering which symbols
// to include when linking static libraries to avoid having the module eliminated as "dead code".
// We need to reference a symbol in each module (crate) that registers an importer since atelier_importer uses
// inventory::submit and the .ctor linkage hack.
// Note that this is only required if you use the built-in `atelier_importer::get_source_importers` to
// register importers with the daemon builder.
fn init_modules() {
    // An example of how referencing of types could look to avoid dead code elimination
    // #[cfg(feature = "amethyst-importers")]
    // {
    //     use amethyst::assets::Asset;
    //     amethyst::renderer::types::Texture::name();
    //     amethyst::assets::experimental::DefaultLoader::default();
    //     let _w = amethyst::audio::output::outputs();
    // }
}

fn main() {
    std::env::set_current_dir(PathBuf::from("examples")).expect("failed to set working directory");
    init_logging().expect("failed to init logging");
    init_modules();

    log::info!(
        "registered importers for {}",
        atelier_importer::get_source_importers()
            .map(|(ext, _)| ext)
            .collect::<Vec<_>>()
            .join(", ")
    );

    let opt = AssetDaemonOpt::from_args();

    AssetDaemon::default()
        .with_importers(atelier_importer::get_source_importers())
        .with_db_path(opt.db_dir)
        .with_address(opt.address)
        .with_asset_dirs(opt.asset_dirs)
        .run();
}
