use atelier_cli::shell::Shell;
use atelier_cli::*;
use tokio::runtime::Runtime;

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new().unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async_main()))
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_context().await?;

    let mut shell = Shell::new(ctx);

    shell.register_command("pack", CmdPack);
    shell.register_command("show_all", CmdShowAll);
    shell.register_command("get", CmdGet);
    shell.register_command("build", CmdBuild);
    shell.register_command("path_for_asset", CmdPathForAsset);
    shell.register_command("assets_for_path", CmdAssetsForPath);

    shell.run_repl().await
}
