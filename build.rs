extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/data.capnp")
        .file("schema/service.capnp")
        .run().expect("schema compiler command");
}