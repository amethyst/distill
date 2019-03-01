// use crate::{BoxedImporter, SimpleImporter};
// use amethyst::{audio, renderer};

// pub fn amethyst_formats() -> Vec<(&'static str, Box<dyn BoxedImporter>)> {
//     vec![
//         ("jpg", Box::new(SimpleImporter::from(renderer::JpgFormat))),
//         ("png", Box::new(SimpleImporter::from(renderer::PngFormat))),
//         ("tga", Box::new(SimpleImporter::from(renderer::TgaFormat))),
//         ("bmp", Box::new(SimpleImporter::from(renderer::BmpFormat))),
//         ("obj", Box::new(SimpleImporter::from(renderer::ObjFormat))),
//         ("wav", Box::new(SimpleImporter::from(audio::WavFormat))),
//         ("ogg", Box::new(SimpleImporter::from(audio::OggFormat))),
//         ("flac", Box::new(SimpleImporter::from(audio::FlacFormat))),
//         ("mp3", Box::new(SimpleImporter::from(audio::Mp3Format))),
//     ]
// }
