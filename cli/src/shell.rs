use async_trait::async_trait;
use tokio::io;
use tokio::prelude::*;
use futures::stream::StreamExt;
use crate::DynResult;
use std::collections::HashMap;

#[async_trait(?Send)]
pub trait Command<C> {
    fn nargs(&self) -> usize {
        0
    }
    fn desc(&self) -> &str;
    async fn run(&self, ctx: &mut C, stdout: &mut io::Stdout, args: Vec<&str>) -> DynResult;
}

pub struct Shell<C> {
    commands: HashMap<&'static str, Box<dyn Command<C>>>,
}

impl<C> Shell<C> {
    pub fn new() -> Self {
        Self { commands: HashMap::new() }
    }

    pub fn register_command(&mut self, keyword: &'static str, cmd: impl Command<C> + 'static) {
        self.commands.insert(keyword, Box::new(cmd));
    }

    pub async fn run_repl(&self, mut ctx: C) -> DynResult {
        let stdin = io::stdin();
        let mut stdout = io::stdout();
    
        let framed_read =
            tokio_util::codec::FramedRead::new(stdin, tokio_util::codec::LinesCodec::new());
        futures::pin_mut!(framed_read);
    
        let commands = &self.commands;
    
        loop {
            stdout.write_all(b"> ").await?;
            stdout.flush().await?;
            if let Some(line) = framed_read.next().await {
                let line = line?;
                let mut splt = line.trim().split_whitespace();
                let cmd_text = splt.next();
                let args = splt.collect::<Vec<&str>>();
                let command = cmd_text.and_then(|cmd| commands.get(cmd));
    
                if let Some(command) = command {
                    if command.nargs() != args.len() {
                        stdout.write_all(b"Invalid number of arguments\n").await?;
                    } else {
                        if let Err(e) = command.run(&mut ctx, &mut stdout, args).await {
                            async_write!(stdout, "Error: {}\n", e).await?;
                        }
                    }
                } else {
                    match cmd_text {
                        Some("help") | Some("?") | Some("h") => {
                            for (cmd_key, cmd) in commands {
                                async_write!(stdout, "  {} {}\n", cmd_key, cmd.desc()).await?;
                            }
                        }
                        Some(_) => stdout.write_all(b"Unknown command\n").await?,
                        None => {},
                    }
                }
            } else {
                break;
            }
        }
    
        Ok(())
    }
}