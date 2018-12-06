extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schemas/data.capnp")
        .file("schemas/service.capnp")
        .run().expect("schema compiler command");
}