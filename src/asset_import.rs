use amethyst::assets::{Asset, Format, Source};
use amethyst::prelude::*;
use amethyst::renderer::TextureMetadata;
use downcast::Any;
use erased_serde::{Deserializer, Serialize};
use std::{fs, io::Read, sync::Arc};

pub trait AnySerialize: Serialize + Any {}

serialize_trait_object!(AnySerialize);

impl<T: Serialize + 'static> AnySerialize for T {}
downcast!(AnySerialize);

pub trait BoxedFormat {
    fn import_boxed(
        &self,
        bytes: Vec<u8>,
        options: Box<AnySerialize>,
    ) -> ::amethyst::assets::Result<Box<AnySerialize>>;
    fn default_metadata(&self) -> Box<AnySerialize>;
}

struct FormatBox<A: Asset + Sized, O: Sized + Send + 'static> {
    pub format: Box<Format<A, Options = O>>,
}

impl<A: Asset + Sized, O: Sized + Send + Serialize + 'static> FormatBox<A, O> {
    pub fn new(format: Box<Format<A, Options = O>>) -> FormatBox<A, O> {
        FormatBox { format: format }
    }
}

struct ByteSource {
    bytes: Vec<u8>,
}

impl Source for ByteSource {
    fn modified(&self, path: &str) -> ::amethyst::assets::Result<u64> {
        Ok(0)
    }
    fn load(&self, path: &str) -> ::amethyst::assets::Result<Vec<u8>> {
        Ok(self.bytes.clone())
    }
}

impl<A: Asset + Sized, O: Sized + Send + Default + Serialize + 'static> BoxedFormat
    for FormatBox<A, O>
where
    <A as Asset>::Data: Serialize,
{
    fn import_boxed(
        &self,
        bytes: Vec<u8>,
        options: Box<AnySerialize>,
    ) -> ::amethyst::assets::Result<Box<AnySerialize>> {
        let source = Arc::new(ByteSource { bytes: bytes });
        Ok(Box::new(
            self.format
                .import(
                    "-".to_string(),
                    source,
                    *options.downcast::<O>().unwrap(),
                    false,
                )?
                .data,
        ))
    }

    fn default_metadata(&self) -> Box<AnySerialize> {
        Box::new(O::default())
    }
}

pub fn format_from_ext(ext: &str) -> Option<Box<BoxedFormat>> {
    match ext {
        "jpg" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::renderer::JpgFormat {},
        )))),
        "png" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::renderer::PngFormat {},
        )))),
        "tga" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::renderer::TgaFormat {},
        )))),
        "bmp" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::renderer::BmpFormat {},
        )))),
        "obj" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::renderer::ObjFormat {},
        )))),
        "wav" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::audio::WavFormat {},
        )))),
        "ogg" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::audio::OggFormat {},
        )))),
        "flac" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::audio::FlacFormat {},
        )))),
        "mp3" => Some(Box::new(FormatBox::new(Box::new(
            ::amethyst::audio::Mp3Format {},
        )))),
        _ => None,
    }
}
