use std::{collections::HashMap, io::Write, pin::Pin};

use async_trait::async_trait;
use crossterm::{
    cursor::{MoveLeft, MoveToColumn, MoveUp},
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    queue,
    style::{Color, Print, ResetColor, SetBackgroundColor, SetForegroundColor},
    terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
};
use futures::{
    future::{pending, FusedFuture, FutureExt},
    select,
    stream::StreamExt,
};

use crate::DynResult;

#[async_trait(?Send)]
pub trait Command<C> {
    fn nargs(&self) -> usize {
        0
    }
    fn desc(&self) -> &str;
    async fn run(&self, ctx: &C, args: Vec<&str>) -> DynResult;
    async fn autocomplete(
        &self,
        ctx: &C,
        args: Vec<&str>,
        whitespaces_last: usize,
    ) -> DynResult<Autocomplete> {
        let _ = (ctx, args, whitespaces_last);
        Ok(Autocomplete {
            overlap: 0,
            items: Vec::new(),
        })
    }
}

pub struct Shell<C> {
    commands: HashMap<&'static str, Box<dyn Command<C>>>,
    ctx: C,
}

#[derive(Debug)]
struct Exit;
impl std::error::Error for Exit {}
impl std::fmt::Display for Exit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exit")
    }
}

impl<C> Shell<C> {
    pub fn new(ctx: C) -> Self {
        Self {
            commands: HashMap::new(),
            ctx,
        }
    }

    pub fn register_command(&mut self, keyword: &'static str, cmd: impl Command<C> + 'static) {
        self.commands.insert(keyword, Box::new(cmd));
    }

    async fn autocomplete(&self, cmdline: String) -> DynResult<Autocomplete> {
        let mut splt = cmdline.split_whitespace();
        let cmd_text = splt.next().unwrap_or("");

        if cmdline.len() == cmd_text.len() {
            Ok(Autocomplete {
                overlap: cmd_text.len(),
                items: self
                    .commands
                    .keys()
                    .filter(|key| key.starts_with(&cmd_text))
                    .map(|s| s.to_string())
                    .collect(),
            })
        } else {
            let commands = &self.commands;
            if let Some(command) = commands.get(cmd_text) {
                let args = splt.collect::<Vec<&str>>();
                let whitespaces_last = cmdline
                    .chars()
                    .rev()
                    .take_while(|c| c.is_whitespace())
                    .count();
                command
                    .autocomplete(&self.ctx, args, whitespaces_last)
                    .await
            } else {
                Ok(Autocomplete {
                    overlap: 0,
                    items: Vec::new(),
                })
            }
        }
    }

    async fn execute_command(&self, cmdline: &str) -> DynResult {
        let mut splt = cmdline.trim().split_whitespace();
        let cmd_text = splt.next();
        let args = splt.collect::<Vec<&str>>();

        let commands = &self.commands;
        let command = cmd_text.and_then(|cmd| commands.get(cmd));

        if let Some(command) = command {
            if command.nargs() != args.len() {
                println!("Invalid number of arguments\r");
            } else if let Err(e) = command.run(&self.ctx, args).await {
                println!("Error: {}\r", e);
            }
        } else {
            match cmd_text {
                Some("help") | Some("?") | Some("h") => {
                    for (cmd_key, cmd) in commands {
                        println!("  {} {}\r", cmd_key, cmd.desc());
                    }
                    println!("  quit\r");
                }
                Some("exit") | Some("quit") => return Err(Exit.into()),
                Some(_) => println!("Unknown command\r"),
                None => {}
            }
        }

        Ok(())
    }

    // async fn handle_event() -> DynResult {

    // }

    pub async fn run_repl(&self) -> DynResult {
        let defer = defer::defer(|| {
            let _ = disable_raw_mode();
        });

        enable_raw_mode()?;

        let mut placeholder_future: Pin<Box<dyn FusedFuture<Output = DynResult<Autocomplete>>>> =
            Box::pin(pending().fuse());

        let mut stdout = std::io::stdout();
        let mut reader = EventStream::new().fuse();
        let mut line = String::with_capacity(128);
        let mut history = Vec::new();
        let mut state = State::Insert;
        let mut fut_autocomplete: Option<
            Pin<Box<dyn FusedFuture<Output = DynResult<Autocomplete>>>>,
        > = None;

        'repl_loop: loop {
            queue!(stdout, Print("\r> "), Print(&line),)?;

            if let State::Select {
                items,
                cursor,
                overlap,
            } = &state
            {
                if *overlap > 0 {
                    queue!(stdout, MoveLeft(*overlap as _))?;
                }
                queue!(
                    stdout,
                    Print(&items[*cursor]),
                    Clear(ClearType::UntilNewLine),
                )?;

                let cursor_position_x = 2 + line.len() + items[*cursor].len() - overlap + 1;

                queue!(stdout, Print("\r\n"))?;

                let max_width = crossterm::terminal::size()?.0 as usize;
                let mut current_width = 0;
                let mut advanced_lines: usize = 1;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        if current_width + item.len() > max_width {
                            queue!(stdout, Print("\r\n"), Clear(ClearType::UntilNewLine),)?;
                            current_width = 0;
                            advanced_lines += 1;
                        } else {
                            queue!(stdout, Print(" "))?;
                            current_width += 1;
                        }
                    }
                    if i == *cursor {
                        queue!(
                            stdout,
                            SetForegroundColor(Color::Black),
                            SetBackgroundColor(Color::White),
                            Print(item),
                            ResetColor,
                        )?;
                    } else {
                        queue!(stdout, Print(item))?;
                    }
                    current_width += item.len();
                }

                queue!(
                    stdout,
                    Clear(ClearType::FromCursorDown),
                    MoveUp(advanced_lines as u16),
                    MoveToColumn(cursor_position_x as u16),
                )?;
            } else {
                queue!(stdout, Clear(ClearType::FromCursorDown),)?;
            }

            stdout.flush()?;

            'event_loop: loop {
                #[derive(Debug)]
                enum Action {
                    Event(Event),
                    Autocomplete(Autocomplete),
                }

                let mut autocomplete = fut_autocomplete.as_mut().unwrap_or(&mut placeholder_future);

                let mut next_action = select! {
                    a = autocomplete => {
                        match a {
                            Ok(a) => Action::Autocomplete(a),
                            Err(e) => {
                                println!("Autocomplete error: {:?}\r", e);
                                break 'event_loop;
                            },
                        }
                    },
                    e = reader.next() => match e {
                        Some(Err(e)) => {
                            println!("Event error: {:?}\r", e);
                            break 'event_loop;
                        },
                        None => break 'repl_loop,
                        Some(Ok(e)) => Action::Event(e),
                    },
                };

                let mut handle_again = true;
                while handle_again {
                    handle_again = false;

                    match (&mut state, &mut next_action) {
                        (_, Action::Event(Event::Resize(_, _))) => break 'event_loop,
                        (State::AwaitAutocomplete, Action::Event(Event::Key(KeyEvent { .. }))) => {
                            state = State::Insert;
                            fut_autocomplete = None;
                            handle_again = true;
                        }
                        (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Esc, ..
                            })),
                        )
                        | (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Char('c'),
                                modifiers: KeyModifiers::CONTROL,
                            })),
                        ) => {
                            break 'repl_loop;
                        }
                        (
                            State::Select { .. },
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Esc, ..
                            })),
                        ) => {
                            state = State::Insert;
                            break 'event_loop;
                        }
                        (
                            State::Select {
                                items,
                                ref mut cursor,
                                ..
                            },
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Tab, ..
                            })),
                        ) => {
                            *cursor = (*cursor + 1) % items.len();
                            break 'event_loop;
                        }
                        (
                            State::Select {
                                items,
                                ref mut cursor,
                                ..
                            },
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::BackTab,
                                ..
                            })),
                        ) => {
                            *cursor = (*cursor + items.len() - 1) % items.len();
                            break 'event_loop;
                        }
                        (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Enter,
                                ..
                            })),
                        ) => {
                            println!("\r");
                            match self.execute_command(&line).await {
                                Err(err) if err.downcast_ref::<Exit>().is_some() => {
                                    break 'repl_loop
                                }
                                Err(err) => return Err(err),
                                Ok(_) => {}
                            }
                            history.push(line.trim().to_owned());
                            line.clear();
                            fut_autocomplete = None;
                            break 'event_loop;
                        }
                        (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Up, ..
                            })),
                        ) => {
                            if line.is_empty() && !history.is_empty() {
                                state = State::History { cursor: 0 };
                                line = history[0].clone();
                                break 'event_loop;
                            }
                        }
                        (
                            State::History { ref mut cursor },
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Up, ..
                            })),
                        ) => {
                            *cursor = (*cursor + 1).min(history.len() - 1);
                            line = history[*cursor].clone();
                            break 'event_loop;
                        }
                        (
                            State::History { ref mut cursor },
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Down,
                                ..
                            })),
                        ) => {
                            *cursor = cursor.saturating_sub(1);
                            line = history[*cursor].clone();
                            break 'event_loop;
                        }
                        (State::History { .. }, _) => {
                            state = State::Insert;
                            handle_again = true;
                        }
                        (
                            State::Select {
                                overlap,
                                items,
                                cursor,
                            },
                            Action::Event(Event::Key(KeyEvent { code, .. })),
                        ) => {
                            line.truncate(line.len() - *overlap);
                            line.push_str(&items[*cursor]);
                            line.push(' ');
                            state = State::Insert;
                            if let KeyCode::Char(c) = code {
                                if *c != ' ' {
                                    line.push(*c);
                                }
                            }
                            break 'event_loop;
                        }
                        (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Backspace,
                                ..
                            })),
                        ) => {
                            line.pop();
                            break 'event_loop;
                        }
                        (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Char(c),
                                ..
                            })),
                        ) => {
                            line.push(*c);
                            print!("{}", c);
                            stdout.flush()?;
                        }
                        (
                            State::Insert,
                            Action::Event(Event::Key(KeyEvent {
                                code: KeyCode::Tab, ..
                            })),
                        ) => {
                            fut_autocomplete =
                                Some(Box::pin(self.autocomplete(line.clone()).fuse()));
                            state = State::AwaitAutocomplete;
                        }
                        (State::AwaitAutocomplete, Action::Autocomplete(autocomplete)) => {
                            match autocomplete.items.len() {
                                0 => {
                                    state = State::Insert;
                                }
                                1 => {
                                    line.truncate(line.len() - autocomplete.overlap);
                                    line.push_str(&autocomplete.items[0]);
                                    line.push(' ');
                                    state = State::Insert;
                                    break 'event_loop;
                                }
                                _ => {
                                    let items =
                                        std::mem::replace(&mut autocomplete.items, Vec::new());
                                    state = State::Select {
                                        overlap: autocomplete.overlap,
                                        items,
                                        cursor: 0,
                                    };
                                    break 'event_loop;
                                }
                            }
                        }
                        (state, action) => {
                            println!("\r\nUnhandled event shell event:\r");
                            println!("State::{:?}\r", state);
                            println!("Action::{:?}\r", action);
                            break 'event_loop;
                        }
                    }
                }
            }
        }

        drop(defer);
        Ok(())
    }
}

#[derive(Debug)]
pub struct Autocomplete {
    pub overlap: usize,
    pub items: Vec<String>,
}

impl Autocomplete {
    pub fn empty() -> Self {
        Autocomplete {
            overlap: 0,
            items: Vec::new(),
        }
    }
}

#[derive(Debug)]
enum State {
    Insert,
    AwaitAutocomplete,
    Select {
        overlap: usize,
        items: Vec<String>,
        cursor: usize,
    },
    History {
        cursor: usize,
    },
}
