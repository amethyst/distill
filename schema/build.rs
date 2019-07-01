extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("schemas/data.capnp")
        .file("schemas/service.capnp")
        .run()
        .expect("schema compiler command");
}
