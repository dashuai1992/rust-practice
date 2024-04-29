// kv.rs
use std::{
  collections::{BTreeMap, HashMap}, env::current_dir, ffi::OsStr, fs::{self, create_dir_all, read_dir, File, OpenOptions}, io::{self, BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write}, ops::Range, path::PathBuf
};

use serde_json::Deserializer;

use self::{
  command::{CmdIdx, Command}, 
  writer::WriterWithPos
};

pub mod command;
pub mod writer;

// 指令数据压缩阈值
const COMPACTION_THRESHOLD: u64 = 1024;

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
  index: BTreeMap<String, CmdIdx>,

  // 未被压缩的指令数据长度
  uncompacted: u64,
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

      // 未被压缩的指令数据长度
      let mut uncompacted = 0;

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
              // 将数据的位置范围记录在Btreemap中
              let cmd_index: CmdIdx = (file_name, Range {start: start_pos, end: end_pos}).into();
              if let Some(cmd_old) = &index.insert(key, cmd_index) {
                // 将旧值长度累加
                uncompacted += cmd_old.len;
              }
            },

            // 匹配到remove命令
            Command::Remove { key } => {
              if let Some(cmd_old) = index.remove(&key) {
                // 将旧值长度累加
                uncompacted += cmd_old.len;  
              }
              // 刚才累加的set的长度，还需要把remove指令的长度也累加上
              uncompacted += end_pos - start_pos;
            },

            // get命令不会在数据文件中
            _ => (),
          }
          // 开始位置就是下个命令的结束位置
          start_pos = end_pos;
        }
        // 每个文件的reader都保存下来，get的时候，根据key找到索引，索引中有文件名和key对应的位置。
        readers.insert(file_name, file_reader);
      }

    // 返回
    Ok(KvStore {
        data_path,
        cur_data_file_name,
        writer,
        readers,
        index,
        uncompacted,
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
      let insert = self.index.insert(key, (self.cur_data_file_name, (start..end)).into());

      // 累加可以合并指令数据长度
      if let Some(cmd_old) = insert {
          self.uncompacted += cmd_old.len;
      }

      // 判断可合并的长度，大于阈值就执行合并方法
      if COMPACTION_THRESHOLD < self.uncompacted {
        self.compact()?;
      }
    }
    
    Ok(())
  }

  pub fn get(&mut self, key: String) -> Result<Option<String>> {
    // 根据key在索引中找到索引数据
    if let Some(cmd_idx) = self.index.get(&key) {
      // 根据索引数据中的文件名找到对应数据文件的reader
      let reader = self.readers.get_mut(&cmd_idx.file).expect("没有找到数据文件！");
      // 移动reader读取数据文件的指针位置，索引中记录的数据的位置
      let _ = reader.seek(SeekFrom::Start(cmd_idx.pos))?;
      // 根据索引记录的数据长度，取出相应的数据
      let take = reader.take(cmd_idx.len);
      // 使用serde_json读取数据转换成Command
      let from_reader = serde_json::from_reader::<_, Command>(take)?;
      // 匹配command::set，能匹配到就返回value字段
      if let Command::Set { value, .. } = from_reader {
          Ok(Some(value))
      } else {
        // 匹配不到command::set
        Ok(None)
      }
    } else {
      // 没有找到key对应的索引
      Ok(None)
    }
  }

  pub fn remove(&mut self, key: String) -> Result<()> {
    // 判断索引中是否包含这个key
    if self.index.contains_key(&key) {
      // 数据的开始位置 
      let start = self.writer.pos;
      // 写入文件
      let cmd_rm = Command::Remove { key };
      serde_json::to_writer(&mut self.writer, &cmd_rm)?;
      self.writer.flush()?;
      // 数据的结束位置
      let end = self.writer.pos;
      // 删除索引数据
      if let Command::Remove { key } = cmd_rm {
          let remove = self.index.remove(&key);
          // 累加长度
          if let Some(cmd_old) = remove {
              self.uncompacted += cmd_old.len;
          }
          // remove指令的长度
          self.uncompacted += end - start;
      }

      Ok(())
    } else {

      // 没有找到返回一个错误
      Err(Error::from(ErrorKind::NotFound))
    }
  }

  fn compact(&mut self) -> Result<()> {
    // 压缩后要写入的文件
    let compaction_file_name = self.cur_data_file_name + 1;
    let compaction_file = OpenOptions::new().append(true).write(true).create(true).open(data_dir()?.join(format!("{}.log", compaction_file_name)))?;
    let mut compaction_writer = WriterWithPos::new(compaction_file)?;
    let compaction_reader = BufReader::new(File::open(data_dir()?.join(format!("{}.log", compaction_file_name)))?);
    self.readers.insert(compaction_file_name, compaction_reader);

    // 新来的数据写入的数据文件，区别于合并压缩过的数据文件
    let cur_data_file_name = compaction_file_name + 1;
    let cur_data_file = OpenOptions::new().append(true).write(true).create(true).open(data_dir()?.join(format!("{}.log", cur_data_file_name)))?;
    self.writer = WriterWithPos::new(cur_data_file)?;
    self.readers.insert(cur_data_file_name, BufReader::new(File::open(data_dir()?.join(format!("{}.log", cur_data_file_name)))?));
    self.cur_data_file_name = cur_data_file_name;

    // 遍历index
    for cmd_idx in &mut self.index.values_mut() {

      // 取出当前索引的reader
      let reader = self.readers.get_mut(&cmd_idx.file).expect("没有找到数据文件！");

      // 将索引对应的数据copy到压缩合并后的新数据文件中
      reader.seek(SeekFrom::Start(cmd_idx.pos))?;
      let mut take = reader.take(cmd_idx.len);
      let start = compaction_writer.pos;
      io::copy(take.by_ref(), compaction_writer.by_ref())?;
      let end = compaction_writer.pos;

      // 索引数据重新赋值，新文件的数据位置
      *cmd_idx = (compaction_file_name, start..end).into();
    }
    // 至此，索引中的数据已经全部转移到了新的文件中，这个新文件就所说的指令数据压缩文件
    compaction_writer.flush()?;
    // 重置uncompacted
    self.uncompacted = 0;

    // 清除旧的数据文件 
    let old_file_names = self.readers
      .keys()
      // 过滤出小于压缩合并文件的文件名，这已经是旧文件了。
      .filter(|&&res| res < compaction_file_name)
      .cloned()
      .collect::<Vec<u32>>();

    for file_name in old_file_names {
      // 删除旧文件的reader
      self.readers.remove(&file_name);
      // 删除旧文件
      fs::remove_file(data_dir()?.join(format!("{}.log",file_name)))?;
    }

    Ok(())
  }

}

fn data_dir() -> Result<PathBuf> {
  Ok(current_dir()?.join("data"))
}

#[cfg(test)]
mod tests {
  use std::{env::current_dir, fs::{File, OpenOptions}, io::{self, BufReader, Read, Result, Seek, Write}};
use serde_json::Deserializer;

use super::{command::Command, writer::WriterWithPos, KvStore};

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
  fn test_get() -> Result<()> {
    let mut open = KvStore::open()?;
    let get = open.get("foo".to_string())?;
    assert_eq!(Some("bar".to_string()), get);
    Ok(())
  }

  #[test]
  fn test_remove() -> Result<()> {
    let mut open = KvStore::open()?;
    let mut is_err = false;
    open.remove("foo1".to_string()).unwrap_or_else(|_| is_err = true);
    assert!(!is_err);
    open.remove("foo10000".to_string()).unwrap_or_else(|_| is_err = true);
    assert!(is_err);
    Ok(())
  }

  #[test]
  fn test_compact() -> Result<()> {
    let mut open = KvStore::open()?;
    for i in 0..1000 {
        open.set("key-foo".to_string(), format!("value-bar-{}", i))?;
    }
    // open.remove(format!("key-foo"))?;
    open.compact()?;
    assert_eq!("value-bar-999".to_string(), open.get("key-foo".to_string())?.expect("错误了。。"));

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
      // let byte_offset = stream_deserializer.byte_offset() as u64;
      if let Command::Set { key, value } = cmd? {
          assert_eq!("key", key);
          assert_eq!("value", value);
      }
    }

    Ok(())
  }

  #[test]
  fn test_copy() -> Result<()> {
    let mut buf_reader = BufReader::new(File::open(current_dir()?.join("data.log"))?);
    let copy_file = OpenOptions::new().append(true).create(true).write(true).open(current_dir()?.join("data.copy.log"))?;
    let mut copy_file_writer = WriterWithPos::new(copy_file)?;

    let end_seek = buf_reader.seek(std::io::SeekFrom::End(0))?;
    let _ = buf_reader.seek(std::io::SeekFrom::Start(0))?;

    let mut take = buf_reader.take(end_seek);

    let start = copy_file_writer.pos;
    let copy_len = io::copy(take.by_ref(), copy_file_writer.by_ref())?;
    let end = copy_file_writer.pos;

    assert_eq!(copy_len, end-start);

    Ok(())
  }
}