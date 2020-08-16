extern crate proc_macro;

use proc_macro::TokenStream;

use quote::quote;
use syn::{parse, LitStr};

use uuid::Uuid;

#[proc_macro]
pub fn asset_uuid(input: TokenStream) -> TokenStream {
    let s = parse::<LitStr>(input)
        .expect("Macro input is not a string")
        .value();
    let bytes = *Uuid::parse_str(s.as_str())
        .expect("Macro input is not a UUID string")
        .as_bytes();

    let expanded = quote! {
        atelier_core::AssetUuid([#(#bytes as u8),*])
    };

    TokenStream::from(expanded)
}
