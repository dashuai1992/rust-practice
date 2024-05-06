use std::ops::Range;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
  #[command(subcommand)]
  pub command: Command,
}


#[derive(Subcommand, Serialize, Deserialize, Debug)]
pub enum Command {
  Set {
    /// key
    key: String,
    /// value
    value: String,
  },
  Get {
    /// key
    key: String,
  },
  Remove {
    /// key
    key: String,
  }
}

pub struct CmdIdx {
  // 索引所在的数据文件
  pub file: u32,
  // 数据开始位置
  pub pos: u64,
  // 数据长度
  pub len: u64,
}

type Idx =(u32, Range<u64>); 

impl From<Idx> for CmdIdx {
    fn from((file, range): Idx) -> Self {
      CmdIdx {file, pos: range.start, len: range.end - range.start} 
    }
} 