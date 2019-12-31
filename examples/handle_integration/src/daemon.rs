use std::path::PathBuf;

use atelier_daemon::AssetDaemon;

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

pub fn run() {
    init_modules();

    log::info!(
        "registered importers for {}",
        atelier_importer::get_source_importers()
            .map(|(ext, _)| ext)
            .collect::<Vec<_>>()
            .join(", ")
    );

    AssetDaemon::default()
        .with_importers(atelier_importer::get_source_importers())
        .with_db_path(".assets_db")
        .with_address("127.0.0.1:9999".parse().unwrap())
        .with_asset_dirs(vec![PathBuf::from("assets")])
        .run();
}
