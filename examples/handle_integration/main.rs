use std::path::PathBuf;
mod custom_asset;
mod daemon;
mod game;
mod image;
mod storage;

fn main() {
    std::env::set_current_dir(PathBuf::from("examples/daemon_with_loader"))
        .expect("failed to set working directory");
    atelier_daemon::init_logging().expect("failed to init logging");
    std::thread::spawn(move || {
        daemon::run();
    });
    game::run();
}
