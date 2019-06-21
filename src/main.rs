


// This is required because rustc does not recognize .ctor segments when considering which symbols
// to include when linking static libraries, so we need to reference a symbol in each module that
// registers an importer since it uses inventory::submit and the .ctor linkage hack.
fn init_modules() {
    #[cfg(feature = "amethyst")]
    {
        use amethyst::assets::Asset;
        amethyst::renderer::types::Texture::name();
        let _w = amethyst::audio::output::outputs();
    }
}

fn main() {
    init_logging().expect("failed to init logging");
    init_modules();

    log::debug!("registered importers for {}", atelier_importer::get_source_importers().map(|i| i.extension).collect::<Vec<_>>().join(", ") );

    AssetDaemon::default()
        .with_importers(atelier_importer::get_source_importers().map(|i| {
            (i.extension, (i.instantiator)())
        }))
        .run();
}
