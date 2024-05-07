use kv::server::KvServer;

fn main() {
  KvServer::new().unwrap().start().unwrap();
}
