use kv::server::{AsynKvServer, KvServer};

fn main() {
  //KvServer::new().unwrap().start().unwrap();
  AsynKvServer::new().unwrap().start().unwrap();
}
