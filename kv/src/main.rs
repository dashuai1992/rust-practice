use clap::Parser;
use kv::kv::command::{Cli, Command};

fn main() {
    let cli = Cli::parse();

   match cli.command {
    Command::Set { key, value } => todo!(),
    Command::Get { key } => todo!(),
    Command::Remove { key } => todo!(),
   }
}
