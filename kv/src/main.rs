use clap::Parser;
use kv::kv::{command::{Cli, Command}, KvStore};

fn main() {
    let cli = Cli::parse();

    let mut kv_store = KvStore::open().unwrap();

   match cli.command {
    Command::Set { key, value } => {
        let _ = &kv_store.set(key, value).unwrap();
    },
    Command::Get { key } => {
        let get = &kv_store.get(key).unwrap();
        match get {
            Some(value) => println!("{value}"),
            None => println!("None"),
        }
    },
    Command::Remove { key } => {
        let _ = &kv_store.remove(key).unwrap();
    },
   }
}
