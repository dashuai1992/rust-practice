use std::{borrow::{Borrow, BorrowMut}, cell::RefCell, collections::{BTreeMap, HashMap}, fs::{self, File}, io::{copy, BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Take, Write}, path::PathBuf, sync::{atomic::{AtomicU32, Ordering}, Arc, Mutex}};

use super::{command::{CmdIdx, Command}, data_dir, data_file_path, load_idx, new_data_file, sorted_file_names, writer::WriterWithPos};

// 指令数据压缩阈值
const COMPACTION_THRESHOLD: u64 = 1024*1024;

#[derive(Clone)]
pub struct AsynKvStore {
  writer: Arc<Mutex<StoreWriter>>,
  reader: StoreReader,
  index: Arc<Mutex<BTreeMap<String, CmdIdx>>>
}

impl AsynKvStore {
  pub fn open() -> Result<Self> {
    let data_path = data_dir()?;
    fs::create_dir_all(&data_path)?;

    let mut index: BTreeMap<String, CmdIdx> = BTreeMap::new();
    let mut readers: HashMap<u32, BufReader<File>> = HashMap::new();
    let mut uncompacted = 0u64;

    let file_names = sorted_file_names(&data_path)?;
    let cur_data_file_name = file_names.last().unwrap_or(&0)+1;
    uncompacted += load_idx(&data_path, file_names, &mut readers, &mut index)?;

    let data_path = Arc::new(data_path);
    let reader = StoreReader {
      safe_point: Arc::new(AtomicU32::new(0)),
      readers: RefCell::new(readers),
      data_path: Arc::clone(&data_path),
    };

    let index = Arc::new(Mutex::new(index));
    let writer = StoreWriter {
      cur_data_file_name,
      uncompacted,
      index: index.clone(),
      writer: new_data_file(&data_path, cur_data_file_name, (&reader).readers.take().borrow_mut())?,
      data_path: Arc::clone(&data_path),
      reader: reader.clone(),
    }; 

    let store = AsynKvStore {
      writer: Arc::new(Mutex::new(writer)),
      reader,
      index,
    };
    Ok(store)
  }

  pub fn set(&self, key: String, value: String) -> Result<()> {
    self.writer.lock().unwrap().set(key, value)
  }

  pub fn get(&self, key: String) -> Result<Option<String>> {
    if let Some(cmd_idx) = self.index.lock().unwrap().get(&key) {
      self.reader.borrow().get(cmd_idx)
    } else {
      Ok(None)
    }
  }

  pub fn remove(&self, key: String) -> Result<()> {
    self.writer.lock().unwrap().remove(key)
  }
}

struct StoreReader {
  readers: RefCell<HashMap<u32, BufReader<File>>>,
  data_path: Arc<PathBuf>,
  safe_point: Arc<AtomicU32>
}

impl StoreReader {
  fn get(&self, cmd_idx: &CmdIdx) -> Result<Option<String>> {
    self.read_and(cmd_idx, |take| {
      let command = serde_json::from_reader::<Take<&mut BufReader<File>>, Command>(take)?;
      if let Command::Set { value, .. } = command {
        Ok(Some(value))
      } else {
        Ok(None)
      }
    })
  }

  fn read_and<F, R>(&self, cmd_idx: &CmdIdx, f: F) -> Result<R>
  where
    F: FnOnce(Take<&mut BufReader<File>>) -> Result<R>
  {
    self.close_stale_handles();

    let mut readers = (&self.readers).borrow_mut();
    if !readers.contains_key(&cmd_idx.file) {
      readers.insert(
        cmd_idx.file, 
        BufReader::new(File::open(data_file_path(&self.data_path, cmd_idx.file)?)?)
      );
    }
    let reader = readers.get_mut(&cmd_idx.file).unwrap();
    reader.seek(SeekFrom::Start(cmd_idx.pos))?;
    f(reader.take(cmd_idx.len))
  }

  fn close_stale_handles(&self) {
    let mut readers = self.readers.borrow_mut();

    if !readers.is_empty() {
      let stales = readers
        .keys()
        .filter(|&&res| res < self.safe_point.load(Ordering::SeqCst))
        .cloned()
        .collect::<Vec<u32>>();

      for stale in stales {
        readers.remove(&stale);
      }
    }
  }
}

impl Clone for StoreReader {
  fn clone(&self) -> Self {
    Self { 
      readers: RefCell::new(HashMap::new()),
      data_path: Arc::clone(&self.data_path),
      safe_point: Arc::clone(&self.safe_point),
    }
  }
}

struct StoreWriter {
  uncompacted: u64,
  writer: WriterWithPos<File>,
  index: Arc<Mutex<BTreeMap<String, CmdIdx>>>,
  cur_data_file_name: u32,
  data_path: Arc<PathBuf>,
  reader: StoreReader,
}

impl StoreWriter {
  fn set(&mut self, key: String, value: String) -> Result<()> {
    let cmd = Command::Set { key, value };
    
    let start = self.writer.pos;
    serde_json::to_writer(&mut self.writer, &cmd)?;
    self.writer.flush()?;
    let end = self.writer.pos;

    if let Command::Set { key, .. } = cmd {
      let insert = self.index.lock().unwrap().insert(key, (self.cur_data_file_name, (start..end)).into());
      if let Some(cmd_old) = insert {
          self.uncompacted += cmd_old.len;
      }
      if COMPACTION_THRESHOLD < self.uncompacted {
        self.compact()?;
      }
    }

    Ok(())
  }

  fn remove(&mut self, key: String) -> Result<()> {
    if self.index.lock().unwrap().contains_key(&key) {
      let idx_key = key.clone();
      let cmd = Command::Remove { key };
    
      let start = self.writer.pos;
      serde_json::to_writer(&mut self.writer, &cmd)?;
      self.writer.flush()?;
      let end = self.writer.pos; 

      if let Some(cmd_idx) = self.index.lock().unwrap().remove(&idx_key) {
        self.uncompacted += cmd_idx.len;
      }
      self.uncompacted += end - start;
      
      if COMPACTION_THRESHOLD < self.uncompacted {
        self.compact()?;
      }

      Ok(())
    } else {
      Err(Error::from(ErrorKind::NotFound))
    }
  }

  fn compact(&mut self) -> Result<()> {
    let compaction_file_name = self.cur_data_file_name + 1;
    let reader = &self.reader;
    let mut readers_take = reader.readers.take();
    let readers = readers_take.borrow_mut();
    let mut compaction_writer = new_data_file(&self.data_path, compaction_file_name, readers)?;

    let cur_data_file_name = compaction_file_name + 1;
    self.writer = new_data_file(&self.data_path, cur_data_file_name, readers)?;
    self.cur_data_file_name = cur_data_file_name;

    let mut index = self.index.lock().unwrap();
    for cmd_idx in index.values_mut() {
      let (mut start, mut end) = (0,0);
      reader.read_and(cmd_idx, |mut take| {
        start = compaction_writer.pos;
        copy(take.by_ref(), compaction_writer.by_ref())?;
        end = compaction_writer.pos;
        Ok(())
      }).unwrap();
      *cmd_idx = (compaction_file_name, start..end).into();
    }
    compaction_writer.flush()?;
    self.uncompacted = 0;

    self.reader.safe_point.store(compaction_file_name, Ordering::SeqCst);
    self.reader.close_stale_handles();

    let old_file_names = sorted_file_names(&self.data_path)?
      .into_iter()
      .filter(|&res| res < compaction_file_name);
    for file_name in old_file_names {
      fs::remove_file(data_file_path(&self.data_path, file_name)?)?;
    }

    Ok(())
  }
}