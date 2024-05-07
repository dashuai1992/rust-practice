use std::{io::{BufReader, BufWriter, Result, Write}, net::TcpStream};

use clap::Parser;
use kv::{kv::command::{Cli, Command}, req::{Request, Response}};
use serde::Deserialize;
use serde_json::{de::IoRead, Deserializer};

const DEFAULT_SERVER_PORT: &str = "127.0.0.1:4000";

struct Connection {
  stream_writer: BufWriter<TcpStream>,
  stream_reader: Deserializer<IoRead<BufReader<TcpStream>>>,
}

impl Connection {
  fn open(port: String) -> Result<Connection> {
    let connect = TcpStream::connect(port)?;
    let stream_writer = BufWriter::new(connect.try_clone()?);
    let stream_reader = Deserializer::from_reader(BufReader::new(connect));

    Ok(Connection {
      stream_writer,
      stream_reader 
    })
  }

  fn get(&mut self, key: String) -> Result<Response> {
    let command = Command::Get { key };
    serde_json::to_writer(&mut self.stream_writer, &Request{ command })?;
    self.stream_writer.flush()?;

    let resp = Response::deserialize(&mut self.stream_reader)?;
    Ok(resp)
  }

  fn set(&mut self, key: String, value: String) -> Result<Response> {
    let command = Command::Set { key, value };
    serde_json::to_writer(&mut self.stream_writer, &Request{ command })?;
    self.stream_writer.flush()?;

    let resp = Response::deserialize(&mut self.stream_reader)?;
    Ok(resp)
  }

  fn remove(&mut self, key: String) -> Result<Response> {
    let command = Command::Remove { key };
    serde_json::to_writer(&mut self.stream_writer, &Request{ command })?;
    self.stream_writer.flush()?;

    let resp = Response::deserialize(&mut self.stream_reader)?;
    Ok(resp)
  }
}

fn main() {
  let parse = Cli::parse();

  let port = parse
    .port
    .or(Some(String::from(DEFAULT_SERVER_PORT)));

  let mut connect = Connection::open(port.unwrap())
    .expect("连接服务器异常！");

  match parse.command {
    Command::Set { key, value } => {
      let set = connect.set(key, value).unwrap();
      println!("{:?}", set);
    },
    Command::Get { key } => {
      let get = connect.get(key).unwrap();
      println!("{:?}", get);
    },
    Command::Remove { key } => {
      let remove = connect.remove(key).unwrap();
      println!("{:?}", remove); 
    },
  }
}