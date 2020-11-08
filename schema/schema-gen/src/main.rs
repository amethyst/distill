fn main() {
    let current_dir = std::env::current_dir().unwrap();
    let parent_dir = current_dir.join("..");
    std::env::set_current_dir(parent_dir).unwrap();
    capnpc::CompilerCommand::new()
        .file("schemas/data.capnp")
        .file("schemas/service.capnp")
        .file("schemas/pack.capnp")
        .output_path("src/")
        .run()
        .expect("schema compiler command");
}
