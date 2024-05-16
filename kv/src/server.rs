
use std::{io::{BufReader, BufWriter, Result, Write}, net::{TcpListener, TcpStream}};

use serde_json::Deserializer;

use crate::{kv::{asyn_store::AsynKvStore, command::Command, KvStore}, req::{Request, Response}, thread_pool::ThreadPool};

const SERVER_PORT: &str = "127.0.0.1:4000";

pub struct KvServer {
  store: KvStore,
}

impl KvServer {
  pub fn new() -> Result<KvServer> {
      Ok(KvServer { store: KvStore::open()? })
  }

  pub fn start(&mut self) -> Result<()> {
    let tcp_listener = TcpListener::bind(SERVER_PORT)?;
    for stream in tcp_listener.incoming() {
      match stream {
        Ok(stream) => {
          if let Err(e) = self.handle_connection(stream) {
            println!("请求错误！{}", e);
          }
        },
        Err(e) => println!("网络连接错误！{}", e),
      }
    }
    Ok(())
  }

  fn handle_connection(&mut self, stream: TcpStream) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    println!("from: {}", peer_addr);

    let reader = Deserializer::from_reader(BufReader::new(&stream));
    let mut writer = BufWriter::new(&stream);

    for reqeust in reader.into_iter::<Request>().flatten() {
      println!("command: {}", serde_json::to_string(&reqeust.command)?);
      match reqeust.command {
        Command::Set { key, value } => {
          let set = self.store
            .set(key, value)
            .map(|_|Some("ok".to_string()))
            .map_err(|e| format!("{e}"));

          serde_json::to_writer(&mut writer, &Response{result: set})?;
          writer.flush()?;
        },
        Command::Get { key } => {
          let get = self.store
            .get(key)
            .map_err(|e| format!("{e}"));

          serde_json::to_writer(&mut writer, &Response{result: get})?;
          writer.flush()?; 
        },
        Command::Remove { key } => {
          let remove = self.store
            .remove(key)
            .map(|_|Some("ok".to_string()))
            .map_err(|e| format!("{e}"));

          serde_json::to_writer(&mut writer, &Response{result: remove})?;
          writer.flush()?;
        },
      }
    }
    Ok(())
  }
}

pub struct AsynKvServer {
  store: AsynKvStore, 
}

impl AsynKvServer {
  pub fn new() -> Result<Self> {
    Ok(AsynKvServer { store: AsynKvStore::open()? })
  }

  pub fn start(&mut self) -> Result<()> {
    let thread_pool = ThreadPool::new(5)?;
    let tcp_listener = TcpListener::bind("127.0.0.1:4000")?;
    for stream in tcp_listener.incoming() {
      let stream = stream?;
      let store = self.store.clone();
      thread_pool.execute(move || {
        handle_connection(store, stream).unwrap();
      })
    }
    Ok(())
  }
}
fn handle_connection(store: AsynKvStore, stream: TcpStream) -> Result<()> {
  let peer_addr = stream.peer_addr().unwrap();
  println!("from: {}", peer_addr);

  let reader = Deserializer::from_reader(BufReader::new(&stream));
  let mut writer = BufWriter::new(&stream);

  for req_cmd in reader.into_iter::<Request>().flatten() {
    println!("command: {}", serde_json::to_string(&req_cmd.command)?); 
    match req_cmd.command {
        Command::Set { key, value } => {
          let set = store
            .set(key, value)
            .map(|_|Some("ok".to_string()))
            .map_err(|e| format!("{e}"));

          serde_json::to_writer(&mut writer, &Response{result: set})?;
          writer.flush()?;
        },
        Command::Get { key } => {
          let get = store
            .get(key)
            .map_err(|e| format!("{e}"));

          serde_json::to_writer(&mut writer, &Response{result: get})?;
          writer.flush()?; 
        },
        Command::Remove { key } => {
          let remove = store
            .remove(key)
            .map(|_|Some("ok".to_string()))
            .map_err(|e| format!("{e}"));

          serde_json::to_writer(&mut writer, &Response{result: remove})?;
          writer.flush()?;
        },
    }
  }

  Ok(())
}




#[cfg(test)]
mod test {
    use std::{io::{self, BufReader, BufWriter, Result, Write}, net::TcpStream};

    use serde::Deserialize;
    use serde_json::Deserializer;

    use crate::{kv::command::Command, req::{Request, Response}};

  #[test]
  fn test_tcp_set() -> io::Result<()> {
    let tcp_stream = TcpStream::connect("127.0.0.1:4000")?;
    let mut writer = BufWriter::new(&tcp_stream);
    let mut reader = Deserializer::from_reader(BufReader::new(&tcp_stream));

    // set
    let value = Command::Set { key: "key".to_string(), value: "value".to_string() };
    serde_json::to_writer(&mut writer, &Request{command: value})?;
    writer.flush()?;
    let resp = Response::deserialize(&mut reader)?;
    assert_eq!(resp.result, Ok(Some("ok".to_string())));

    // get
    let value = Command::Get { key: "key".to_string() };
    serde_json::to_writer(&mut writer, &Request{command: value})?;
    writer.flush()?;
    let resp = Response::deserialize(&mut reader)?;
    assert_eq!(resp.result, Ok(Some("value".to_string())));

    // remove
    let value = Command::Remove { key: "key".to_string() };
    serde_json::to_writer(&mut writer, &Request{command: value})?;
    writer.flush()?;
    let resp = Response::deserialize(&mut reader)?;
    assert_eq!(resp.result, Ok(Some("ok".to_string())));

    Ok(())
  }

  #[test]
  fn test_asyn_server() -> Result<()> {
    let tcp_stream = TcpStream::connect("127.0.0.1:4000")?;
    let mut writer = BufWriter::new(&tcp_stream);
    let mut reader = Deserializer::from_reader(BufReader::new(&tcp_stream));

    // set
    for i in 0..100 {
      let value = Command::Set { key: format!("foo{}", i), value: format!("bar{}", i) };
      serde_json::to_writer(&mut writer, &Request{command: value})?;
      writer.flush()?;
      let resp = Response::deserialize(&mut reader)?;
      assert_eq!(resp.result, Ok(Some("ok".to_string()))); 
    }

    // get
    for i in 0..100 {
      let value = Command::Get { key: format!("foo{}", i) };
      serde_json::to_writer(&mut writer, &Request{command: value})?;
      writer.flush()?;
      let resp = Response::deserialize(&mut reader)?;
      assert_eq!(resp.result, Ok(Some(format!("bar{}", i)))); 
    }

    // remove
    // for i in 0..100 {
    //   let value = Command::Remove { key: format!("foo{}", i) };
    //   serde_json::to_writer(&mut writer, &Request{command: value})?;
    //   writer.flush()?;
    //   let resp = Response::deserialize(&mut reader)?;
    //   assert_eq!(resp.result, Ok(Some("ok".to_string()))); 
    // }

    Ok(())
  }
}