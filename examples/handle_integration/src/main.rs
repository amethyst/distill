mod custom_asset;
mod daemon;
mod game;
mod image;
mod storage;

fn main() {
    atelier_daemon::init_logging().expect("failed to init logging");
    std::thread::spawn(move || {
        daemon::run();
    });
    game::run();

    println!("Successfully loaded and unloaded assets.");
    println!(
        r#"Check the asset metadata using the CLI! 
Open a new terminal without exiting this program, and run:
- `cd cli` # from the project root
- `cargo run`
- Try `show_all` to get UUIDs of all indexed assets, then `get` a returned uuid
- `help` to list all available commands. 
"#
    );
    use std::io::{Read, Write};
    let mut stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    write!(stdout, "Press any key to exit...").unwrap();
    stdout.flush().unwrap();

    let _ = stdin.read(&mut [0u8]).unwrap();
}
