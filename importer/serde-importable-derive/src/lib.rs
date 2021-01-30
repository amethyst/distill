extern crate proc_macro;

use quote::quote;
use syn::*;

#[proc_macro_derive(SerdeImportable, attributes(uuid))]
pub fn serde_importable_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast: DeriveInput = syn::parse(input).unwrap();

    // Build the trait implementation
    let name = &ast.ident;

    let mut uuid = None;
    for attribute in ast.attrs.iter().filter_map(|attr| attr.parse_meta().ok()) {
        let name_value = if let Meta::NameValue(name_value) = attribute {
            name_value
        } else {
            continue;
        };

        if name_value.path.get_ident().unwrap() != "uuid" {
            continue;
        }

        let uuid_str = match name_value.lit {
            Lit::Str(lit_str) => lit_str,
            _ => panic!("uuid attribute must take the form `#[uuid = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"`"),
        };

        uuid = Some(uuid_str);
    }

    let uuid =
        uuid.expect("No `#[uuid = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"` attribute found");
    let gen = quote! {
        #[distill_importer::typetag::serde(name = #uuid)]
        impl distill_importer::SerdeImportable for #name {
        }
    };
    gen.into()
}
