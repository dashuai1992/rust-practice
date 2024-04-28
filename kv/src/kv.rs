// kv.rs
use std::{
  collections::{BTreeMap, HashMap}, 
  env::current_dir, 
  ffi::OsStr, 
  fs::{create_dir_all, read_dir, File, OpenOptions}, 
  io::{BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
  ops::Range, path::PathBuf
};

use serde_json::Deserializer;

use self::{
  command::{CmdIdx, Command}, 
  writer::WriterWithPos
};

pub mod command;
pub mod writer;

/// KvStore, 存储键值对的上下文结构体
struct KvStore {
  // 数据文件的位置
  data_path: PathBuf,

  // 当前正在操作的数据文件
  // 数据文件的命名方式使用数字递增的方式 1.log, 2.log, 3.log。。。
  cur_data_file_name: u32,

  // 当前数据文件的writer
  writer: WriterWithPos<File>,

  // 数据文件路径下所有文件reader
  // 使用hashmap来存，key: 文件名, value: writer
  readers: HashMap<u32, BufReader<File>>,

  // 数据索引
  index: BTreeMap<String, CmdIdx>
}

impl KvStore {
  // 初始化KvStore
  pub fn open() -> Result<KvStore> {
    // 数据文件路径
    // current_dir/data
    let data_path = data_dir()?;

    // 创建目录
    create_dir_all(&data_path)?;

    // 读取数据文件目录所有的文件，
    // 过滤，只要.log结尾的文件
    // 只要数字开头的文件
    let mut file_names: Vec<u32> = read_dir(&data_path)?
      // 展开PathBuf
      .flat_map(|res| Ok(res?.path()) as Result<PathBuf>)
      // 过滤出.log文件 
      .filter(|res| res.is_file() && res.extension() == Some("log".as_ref()))
      // 从路径中取出文件名
      .flat_map(|res| {
        res
          // 文件名
          .file_name()
          // 转系统字符
          .and_then(OsStr::to_str)
          // 去掉后缀
          .map(|res| res.trim_end_matches(".log"))
          // 转u32
          .map(str::parse::<u32>)
      })
      // 展开
      .flatten()
      .collect();

      // 文件名数字排序
      file_names.sort();

      // 当前正在操作的数据文件名，从所有的文件中取出最大的，+1。
      let cur_data_file_name = file_names.last().unwrap_or(&0) + 1;
      // 文件路径
      let cur_data_file_path = data_path.join(format!("{}.log", cur_data_file_name));

      // writer, 文件已经创建
      let writer = WriterWithPos::new(
        OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&cur_data_file_path)?
      )?;

      // writers
      let mut readers = HashMap::new();
      readers.insert(cur_data_file_name, BufReader::new(File::open(cur_data_file_path)?));

      // 内存中的数据索引
      let mut index = BTreeMap::new();

      // 从所有的数据文件中加载数据到索引中
      for file_name in file_names {
        // 每个文件的reader
        let file_path = data_path.join(format!("{}.log", file_name));
        let file = File::open(file_path)?;
        let mut file_reader = BufReader::new(file);
      
        // 从文件开始位置读
        let mut start_pos = file_reader.seek(SeekFrom::Start(0))?;
        // 按Command的json格式读
        let mut from_reader = Deserializer::from_reader(file_reader.by_ref()).into_iter::<Command>();
        while let Some(cmd) = from_reader.next() {
          // command的结束位置
          let end_pos = from_reader.byte_offset() as u64;
          match cmd? {

            // 匹配到set命令
            Command::Set { key, .. } => {
              let cmd_index: CmdIdx = (file_name, Range {start: start_pos, end: end_pos}).into();
              let _ = &index.insert(key, cmd_index);
            },

            // 匹配到remove命令
            Command::Remove { key } => todo!(),

            // get命令不会在数据文件中
            _ => (),
          }
          // 开始位置就是下个命令的结束位置
          start_pos = end_pos;
        }
        readers.insert(file_name, file_reader);
      }

    // 返回
    Ok(KvStore {
        data_path,
        cur_data_file_name,
        writer,
        readers,
        index,
    })
  }

  /// set
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    // set命令对象
    let cmd = Command::Set { key, value };

    // 数据开始位置
    let start = self.writer.pos;

    // 写入json到文件
    serde_json::to_writer(self.writer.by_ref(), &cmd)?;
    self.writer.flush()?;

    // 数据结束位置
    let end = self.writer.pos;

    // 将数据插入到内存索引中
    if let Command::Set { key, .. } = cmd {
      self.index.insert(key, (self.cur_data_file_name, (start..end)).into());
    }
    
    Ok(())
  }

  pub fn get(&mut self, key: String) -> Result<String> {
    if let Some(cmd_idx) = self.index.get(&key) {
      let reader = self.readers.get_mut(&cmd_idx.file).expect("没有找到数据文件！");
      let seek = reader.seek(SeekFrom::Start(cmd_idx.pos))?;
      let take = reader.take(cmd_idx.len);
      let from_reader = serde_json::from_reader::<_, Command>(take)?;
      if let Command::Set { key, value } = from_reader {
          Ok(value)
      } else {
        Err(Error::from(ErrorKind::UnexpectedEof))
      }
    } else {
      Err(Error::from(ErrorKind::UnexpectedEof))
    }
  }
}

fn data_dir() -> Result<PathBuf> {
  Ok(current_dir()?.join("data"))
}

#[cfg(test)]
mod tests {
  use std::{env::current_dir, fs::File, io::{BufReader, Result}};
use serde_json::Deserializer;

use super::{command::Command, KvStore};

  #[test]
  fn test_set() -> Result<()> {
    let mut kvs = KvStore::open()?;
    let _ = kvs.set("key".to_string(), "value".to_string())?;
    Ok(())
  }

  #[test]
  fn test_open_set() -> Result<()> {
    let mut open = KvStore::open()?;
    let _ = open.set("foo".to_string(), "bar".to_string());
    assert_eq!(1, open.index.len());
    let _ = open.set("foo1".to_string(), "bar1".to_string());
    assert_eq!(2, open.index.len());
    let _ = open.set("foo2".to_string(), "bar2".to_string());
    assert_eq!(3, open.index.len());
    Ok(())
  }

  #[test]
  fn test_json_reader() -> Result<()> {
    let join = current_dir()?.join("data.log");
    let file = File::open(join)?;
    let reader = BufReader::new(file);
    let from_reader = Deserializer::from_reader(reader);
    let mut stream_deserializer = from_reader.into_iter::<Command>();

    while let Some(cmd) = stream_deserializer.next() {
      let byte_offset = stream_deserializer.byte_offset() as u64;
      if let Command::Set { key, value } = cmd? {
          assert_eq!("key", key);
          assert_eq!("value", value);
      }
    }

    Ok(())
  }
}