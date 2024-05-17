
# rust-practice

学习rust的代码练习项目


## K/V 存储系统

类似redis的那种key/value数据存储系统。

## 目标是什么呢？

实现3个指令`set`，`get`，`remove`。  

- `set`: 例如`set foo bar`，存储一个数据，key是`foo`，值是`bar`。
  
- `get`: 例如`get foo`，将会得到`bar`的返回值。
  
- `remove`: 例如`remove foo`，将会将`foo`的这组键值对删除。

## 实现大致思路

使用`set`指令时，将指令以`{"set": {"key": "foo", "value": "bar"}}`这样的json形式存储到内存中，同时也考虑存储到文件中，以便下次启动时能够重新回放之前的数据。  
内存中存放的是`key`相关的索引，当使用`get`指令时，根据`key`的值来访问索引，拿到索引后，还是要从文件中来读取数据。  
执行`remove`命令时，需要将`key`相关的数据从内存中删除，并且也要记录到文件中。  

## 那就开始吧

万事开头难，那就边做边想吧。这只是个代码练习的项目，毕竟还只在学习rust没几天，当然是需要深度依赖github上其它的项目代码，多看多写，加油。

### 创建rust项目

```shell
> cargo new kv

     Created binary (application) `kv` package
```

找个合适的目录，执行以上命令，创建一个rust项目。  
然后呢，就是进入到这个项目目录中去。

```shell
> cd kv
> cargo run

   Compiling kv v0.1.0 (C:\Users\96981\Desktop\code\rust-space\kv)
    Finished dev [unoptimized + debuginfo] target(s) in 0.60s
     Running `target\debug\kv.exe`
Hello, world!
```

此时，一个hello world的rust的项目就完事了。

### 开搞

创建一个lib.rs文件和一个kv.rs文件，目前纯野生Rustacean，不知道这种文件结构方式是不是野路子。  

```rust
// lib.rs
pub mod kv;
```

主要的逻辑大部分都是在kv.rs中编写的。  
首先，我们需要有一个结构体来保存我们服务的上下文，这里称这为KvStore，这个服务对象有set,get,remove这三个主要的方法。大概是这个样子。这里先来实现set方法。

```rust
// kv.rs
use std::io;

/// KvStore, 存储键值对的上下文结构体
struct KvStore {

}

impl KvStore {
    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        
        Ok(())
    }
}
```

### 添加crate

接下来就是在调用set方法时，需要把set指令给封装成一个结构体，这样可以更加方便的序列化到文件中，序列化的库当然应该就是serde了吧。还有命令行工具clap。在控制台执行： 
```shell
cargo add serde serde_json clap
```
并在features添加derive。就像这样：
```toml
# Cargo.toml

...省略...

[dependencies]
clap = { version = "4.5.4", features = ["derive"] }
serde = { version="1.0.198", features=["derive"] }
serde_json = "1.0.116"
```
期望中的命令结构体大概是这样的：
```rust
// kv/command.rs
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
```
命令行的解析的main文件中处理下：
```rust
// main.rs
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
```

现在执行一下cargo run看下效果吧。
```shell
cargo run -- --help

warning: `kv` (bin "kv") generated 4 warnings (run `cargo fix --bin "kv"` to apply 4 suggestions)
    Finished dev [unoptimized + debuginfo] target(s) in 5.63s
     Running `target/debug/kv --help`
Usage: kv <COMMAND>

Commands:
  set     
  get     
  remove  
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

> 好了，架子大概应该可能也许就这样了，对于我这样一下纯野生的Rustacean，内心也是比较忐忑，也不知这样写道对不对，算了，就这样向下写吧。  

### set方法

还是继续接着set方法往下写。  
如果生写，大概就是这样的，倒是也比较简单清晰。  
```rust
  pub fn set(&mut self, key: String, value: String) -> Result<usize> {
    // 命令实例
    let set_cmd = Command::Set { key, value };
    // 当前项目路径
    let cur_dir = current_dir()?;
    // 数据文件路径
    let data_file_path = cur_dir.join("data.log".to_string());
    // 数据文件实例
    let data_file = File::open(data_file_path)?;
    // bufferWriter
    let writer = BufWriter::new(data_file);
    // 写入json
    serde_json::to_writer(writer, &set_cmd)?;

    Ok(serde_json::to_string(&set_cmd)?.len())
  }
```
再测试一下：
```rust
  #[test]
  fn test_set() -> Result<()> {
    let mut kvs = KvStore{};
    let len = kvs.set("key".to_string(), "value".to_string())?;
    assert_ne!(0, len);
    Ok(())
  }
```

执行了一下测试方法，果不其然，没有通过，但是错误信息也是非常直观的，它告诉我，没有这样的文件路径：
```txt
---- kv::tests::test_set stdout ----
Error: Os { code: 2, kind: NotFound, message: "No such file or directory" }


failures:
    kv::tests::test_set

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p kv --lib`

 *  The terminal process "cargo 'test', '--package', 'kv', '--lib', '--', 'kv::tests::test_set', '--exact', '--show-output'" terminated with exit code: 101. 
 *  Terminal will be reused by tasks, press any key to close it. 
```
那就给它创建一个文件不就行了么，真是个小聪明，这样改一下：
```rust
// 数据文件实例
let data_file = File::open(&data_file_path).unwrap_or(File::create(data_file_path)?);
```
再执行一下测试方法，这次就通过了。
```txt
   Compiling kv v0.1.0 (/Users/yuandashuai/Documents/yds/vscode-rust-space/rust-practice/kv)
    Finished test [unoptimized + debuginfo] target(s) in 0.75s
     Running unittests src/lib.rs (target/debug/deps/kv-18d9b559d3ffc8da)

running 1 test
test kv::tests::test_set ... ok

successes:

successes:
    kv::tests::test_set

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```
顺带要检查下data.log这个文件，里面的内容是不是我们期望的json数据。
```json
// data.log
{"Set":{"key":"key","value":"value"}}
```

### 重构它

很明显的，不能这样生写，得想办法让它更加的结构化，更加的优雅。  
KvStore这个结构体之所以被称之主为上下文对象，它应该有一些内置属性或方法，比如：数据文件的读写操作，将set进来或从文件读到的数据的索引给保存在内存中。  
数据索引的话就直接用BTreeMap吧，结构就是`(key: 键, data: (数据文件名, 命令数据范围(开始位置, 结束位置)))`这样式儿的，目前大概就是这些吧。搞下试试。  
定义一个数据索引的结构体：
```rust
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
```
改完的KvStore大概就是这个样子：
```rust
struct KvStore {
  // 数据文件的位置
  data_path: PathBuf,

  // 当前正在操作的数据文件
  // 数据文件的命名方式使用数字递增的方式 1.log, 2.log, 3.log。。。
  cur_data_file_name: u32,

  // 当前数据文件的writer
  writer: BufWriter<File>,

  // 数据文件路径下所有文件reader
  // 使用hashmap来存，key: 文件名, value: writer
  readers: HashMap<u32, BufReader<File>>,

  // 数据索引
  index: BTreeMap<String, CmdIdx>
}
```
再给它加个open方法，相当于初始化它：
```rust
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
      let writer = BufWriter::new(
        OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&cur_data_file_path)?
      );

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
```
set方法我们也要改一下：
```rust
  /// set
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    // set命令对象
    let cmd = Command::Set { key, value };

    // 写入json到文件
    serde_json::to_writer(self.writer.get_ref(), &cmd)?;
    self.writer.flush()?;

    // 将数据插入到内存索引中
    if let Command::Set { key, .. } = cmd {
      self.index.insert(key, (self.cur_data_file_name, 0..0).into());
    }
    
    Ok(())
  }
```
当开始重新写这个set方法时我意识到了问题，那就是在将数据插入到内存索引中去的时候，这里并不能非常直观的获取到数据索引的开始位置和结束位置，虽然说也有其它办法能获取到，比如重新读这个文件，能轻易的获取到这些数据，但是这样做会不会有点太繁杂了呢，还是那名话，需要想个办法，让它更加结构化，更加优雅一点。那么该怎么做呢。  
什么情况下需要去获取数据索引的位置呢？
1. 写文件，将数据索引保存在内存中。
2. 读文件，加载数据文件时，需要从数据文件回放数据，然后将数据索引保存在内存中。  

加载文件时，因为是正在读文件，所以获取数据的位置是很容易的，到目前为止应该是不用特别处理。但是写文件不一样，这里要做的就是在每次写入的时候，把写入后的位置给保存下来。

把writer拿出来单独封装：
```rust
use std::io::{BufWriter, Result, Seek, SeekFrom, Write};

/// 就如effective rust里说的那样，远离过度优化的诱惑，其实File已经实现了Write 和 Seek，我觉得完全可以代替bufwriter,但既然是在练习rust，能多写点就多写点吧。
pub struct WriterWithPos<W: Write + Seek> {

  // 提供写功能的对象其实还是BufWriter
  writer: BufWriter<W>,

  // 每次写完的位置
  pub pos: u64,
}

impl<W: Write + Seek> WriterWithPos<W> {
  pub fn new(mut inner: W) -> Result<Self> {

    // 接受一个实现Write 和 Seek接口的对象，指针调整到最后位置，后写入的数据依次累加进来
    let pos = inner.seek(SeekFrom::End(0))?;

    // 提供写功能的对象其实还是BufWriter
    Ok(WriterWithPos {
      writer: BufWriter::new(inner),
      pos,
    })
  }
}

impl<W: Write + Seek> Write for WriterWithPos<W> {
  fn write(&mut self, buf: &[u8]) -> Result<usize> {

    // 写入的数据长度
    let write_len = self.writer.write(buf)?;
    // 累加到写入文件的位置上
    self.pos += write_len as u64;

    Ok(write_len)
  }

  fn flush(&mut self) -> Result<()> {
    self.writer.flush()?;
    Ok(())
  }
}
```

然后set方法就可以这样来写了： 
```rust
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
}
```

得益于rust方便的测试环境，我可以在很多库不熟悉的情况下，写一些单元测试来验证它，同时，这对学习rust的标准库有很大的帮助，尽管写的测试代码并不是那么符合单元测试的规范。

```rust
#[cfg(test)]
mod tests {

  #[test]
  fn test_seek() -> io::Result<()> {
    let mut file = File::open(current_dir()?.join("data.log"))?;
    let seek_end = file.seek(io::SeekFrom::End(0))?;
    let seek_start = file.seek(io::SeekFrom::Start(0))?;
    let seek_cur = file.seek(io::SeekFrom::Current(0))?;

    println!("seek start: {}, end: {}, current: {}", seek_start, seek_end, seek_cur);

    Ok(())
  }

 #[test]
  fn test_open_set() -> Result<()> {
    let mut open = KvStore::open()?;

    open.set("foo".to_string(), "bar".to_string());
    assert_eq!(1, open.index.len());
    open.set("foo1".to_string(), "bar1".to_string());
    assert_eq!(2, open.index.len());
    open.set("foo2".to_string(), "bar2".to_string());
    assert_eq!(3, open.index.len());
    Ok(())
  }
}
```

执行一下`test_open_set`的测试方法，该测试在我的环境下确实是通过了：
```txt
running 1 test
test kv::tests::test_open_set ... ok

successes:

successes:
    kv::tests::test_open_set

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s
```

此时产生的数据文件是这样的：
```json
// 1.log
{"Set":{"key":"foo","value":"bar"}}{"Set":{"key":"foo1","value":"bar1"}}{"Set":{"key":"foo2","value":"bar2"}}
```
到这里，set方法大概齐了就。

> 可是，这个open方法确实是写的太长了，也是属于一股脑儿往下写的那种，不过，其中有一部分想法是，能分步换行写就一步一步地往下写，你可能会发现，代码中代码中使用函数式编程的风格偶尔会有，但是相对有些比较复杂的地方就很少使用，个人觉得这样做是有一点好处的，就是可以清晰的看到每个步骤的返回类型，这样有助于理解标准库中的api，如果对api非常熟练，那就当我没说。后面想办法把它给抽抽，拆拆。

### get方法

开始写下set方法，该方法的大概逻辑就很简单了，方法参数就是key，拿着key去索引里找索引数据，索引数据的内容包含了数据所在的文件名，数据在文件中的位置等信息。  
找到索引数据，根据数据所在的文件名找到文件对应的reader，有了reader，就可以根据索引记录的数据位置和长度取出对应的数据，再使用serde_json转换成对应的结构体，就可以拿到相应的数据了。写文字描述感觉还挺简单。  

```rust
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
```

顺带测试一下，测试结果告诉我貌似没问题：
```rust
  #[test]
  fn test_get() -> Result<()> {
    let mut open = KvStore::open()?;
    let get = open.get("foo".to_string())?;
    assert_eq!(Some("bar".to_string()), get);
    Ok(())
  }
```

那接下来就是remove方法了。

### remove方法

remove方法是本质是要根据key把对应的数据给删除，首先我们要删除的索引中的数据，然后将remove命令写入到数据文件，文件中不删除任何数据，这就需要KvStore在加载数据文件时回放数据，将所有的命令都执行一遍，就能保证数据的准确性。这感觉上是有问题的呀，算了，先这样写这个remove方法，后边想办法将数据文件中的指令合并。

```rust
  pub fn remove(&mut self, key: String) -> Result<()> {
    // 判断索引中是否包含这个key
    if self.index.contains_key(&key) {
      // 写入文件
      let cmd_rm = Command::Remove { key };
      serde_json::to_writer(&mut self.writer, &cmd_rm)?;
      self.writer.flush()?;
      // 删除索引数据
      if let Command::Remove { key } = cmd_rm {
          self.index.remove(&key);
      }

      Ok(())
    } else {

      // 没有找到返回一个错误
      Err(Error::from(ErrorKind::NotFound))
    }
  }
```

open方法的中加载文件数据加载到remove指令时，需要删除索引：
```rust
pub fn open() -> Result<KvStore> {
    // ...省略...

            // 匹配到set命令
            Command::Set { key, .. } => {
              // 将数据的位置范围记录在Btreemap中
              let cmd_index: CmdIdx = (file_name, Range {start: start_pos, end: end_pos}).into();
              let _ = &index.insert(key, cmd_index);
            },

            // 匹配到remove命令
            Command::Remove { key } => {
              index.remove(&key);
            },

    // ...省略...
}
```

测试一下
```rust
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
```

### 指令数据合并

在写remove方法的时候，发现，只是将指令写入文件，将索引数据删除，重新加载的时候，可能还需要回放好多无用已经删除的指令，所以，这里需要将指令给合并，也就是说set相同key时，数据只需要和最后一次一致即可，remove也是一样，remove指令之前相key的数据都应该是无效的数据。  
那要怎么做呢？  
这里期望是在KvStore的结构体中加入一个字段，用来标识当前可以合并的指令数据的长度，每次set时，判断索引中是否已经有过该key的数据了，如果有，就将这个字段累加上旧值的长度，remove时也是如此，最后，设置一个阈值，当这个字段的长度超过这个阈值时，将执行指令数据合并。

定义阈值：
```rust
// 指令数据压缩阈值
const COMPACTION_THRESHOLD: u64 = 1024;
```

给KvStore添加字段记录没有被压缩的指令数据长度：
```rust
struct KvStore {

  ...

  // 数据索引
  index: BTreeMap<String, CmdIdx>,

  // 未被压缩的指令数据长度
  uncompacted: u64,
}
```

在open方法里，修改回放索引数据的代码：
```rust
    ...

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

    ...
```

每次set时累加uncompacted字段
```rust
...

    // 将数据插入到内存索引中
    if let Command::Set { key, .. } = cmd {
      let insert = self.index.insert(key, (self.cur_data_file_name, (start..end)).into());

      // 累加可以合并指令数据长度
      if let Some(cmd_old) = insert {
          self.uncompacted += cmd_old.len;
      }

      // 判断可合并的长度，大于阈值就执行合并方法
      if COMPACTION_THRESHOLD < self.uncompacted {
        todo!("执行合并指令数据的方法");
      }
    }

...
```

每次remove时累加uncompacted字段
```rust
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
```

好了，uncompacted的累加已经处理完了，剩下的就是压缩合并的方法了，因为上面还有留着一个`todo!()`。

### 压缩合并方法

这里压缩数据文件时，以索引中的数据为准，将索引中对应的数据从文件中取出，写入新的数据文件中，修改索引数据中的数据位置，然后旧的数据文件就可以删除了。

写一个测试方法，测试一下`io::copy()`的表现与期望的是否一致：
```rust
  #[test]
  fn test_copy() -> Result<()> {
    let mut buf_reader = BufReader::new(File::open(current_dir()?.join("data.log"))?);
    let copy_file = OpenOptions::new().append(true).create(true).write(true).open(current_dir()?.join("data.copy.log"))?;
    let mut copy_file_writer = WriterWithPos::new(copy_file)?;

    let end_seek = buf_reader.seek(std::io::SeekFrom::End(0))?;
    let _ = buf_reader.seek(std::io::SeekFrom::Start(0))?;

    let mut take = buf_reader.take(end_seek);

    let start = copy_file_writer.pos;
    let copy_len = io::copy(take.get_mut(), copy_file_writer.by_ref())?;
    let end = copy_file_writer.pos;

    assert_eq!(copy_len, end-start);

    Ok(())
  }
```

给KvStore添加一个`compact()`的方法，开始编写这个方法，用来合并压缩数据文件：
```rust
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
```

这个方法看起来相当窒息，和`KvStore::open()`一样，至少目前看起来是这样的，后面再重构它，没事。  
添加一个测试方法来验证一下它是否有效：

```rust
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
```

从测试结果来看，效果还是不错的，那就把它放在set方法里就行了  

```rust
      // 判断可合并的长度，大于阈值就执行合并方法
      if COMPACTION_THRESHOLD < self.uncompacted {
        self.compact()?;
      }
```

### 命令行处理

把main方法修改一下，匹配每个命令时执行KvStore的方法：
```rust
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
```

在控制台试下：

```shell
cargo run -- set foo bar
cargo run -- get foo
bar
cargo run -- remove foo
cargo run -- get foo
None
```

### 打包

```shell
cargo build --release
```

执行一下打包过的二进制文件

```shell
./kv set foo bar
```
都已经到打包的步骤了，就算是已经完成了，并不是这样的，上面写的代码虽然可以正常的运行，但是还是存在一些待处理的问题，比如：代码结构太随意，其实还有很大的调整空间；每次执行命令就会生成一个日志文件。这些问题都是应该要被提上日程的。

### 又双叒叕要重构了

稍微给代码结构简单地拆一拆，现在`open`和`compact`这两方法太大一坨了。  
这里并没有做太多操作，就是把大方法拆了拆，重复比较多的代码抽出来，最终是这样的：
```rust
// 指令数据压缩阈值
const COMPACTION_THRESHOLD: u64 = 1024;

/// KvStore, 存储键值对的上下文结构体
pub struct KvStore {
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
    let data_path = data_dir()?;
    // 从数据目录中读出文件名，并按数字大小排序，以便计算最新的数据文件名
    let sorted_file_names = sorted_file_names(&data_path)?;
    // 当前正在操作的数据文件名，从所有的文件中取出最大的，+1。
    let cur_data_file_name = sorted_file_names.last().unwrap_or(&0) + 1;
    // readers
    let mut readers = HashMap::new();
    // 内存中的数据索引
    let mut index = BTreeMap::new();
    // 未被压缩的指令数据长度
    let mut uncompacted = 0;
    uncompacted += load_idx(&data_path, sorted_file_names, &mut readers, &mut index)?;
    // writer, 顺带把reader也给创建放入readers中
    let writer = new_data_file(&data_path, cur_data_file_name, &mut readers)?;
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
    let mut compaction_writer = new_data_file(&self.data_path, compaction_file_name, &mut self.readers)?;
    // 新来的数据写入的数据文件，区别于合并压缩过的数据文件
    let cur_data_file_name = compaction_file_name + 1;
    self.writer = new_data_file(&self.data_path, cur_data_file_name, &mut self.readers)?;
    // 重新设置当前的数据文件
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
  // 数据文件路径
  // current_dir/data
  let data_path = current_dir()?.join("data");
  // 创建目录
  create_dir_all(&data_path)?;
  Ok(data_path)
}

fn sorted_file_names(data_path: &PathBuf) -> Result<Vec<u32>> {
  // 读取数据文件目录所有的文件，
  // 过滤，只要.log结尾的文件
  // 只要数字开头的文件
  let mut file_names: Vec<u32> = read_dir(data_path)?
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

    Ok(file_names)
}

fn data_file_path(path: &PathBuf, file_name: u32) -> Result<PathBuf> {
  Ok(path.join(format!("{}.log", file_name)))
}

fn new_data_file(dir: &PathBuf, file_name: u32, readers: &mut HashMap<u32, BufReader<File>>) -> Result<WriterWithPos<File>> {

  // 文件路径
  let file_path = data_file_path(dir, file_name)?;

  // writer, 文件已经创建
  let writer = WriterWithPos::new(
    OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .append(true)
    .open(&file_path)?
  )?;
  readers.insert(file_name, BufReader::new(File::open(file_path)?));

  Ok(writer)
}

fn load_idx(dir: &PathBuf, 
  file_names: Vec<u32>, 
  readers: &mut HashMap<u32, BufReader<File>>, 
  index: &mut BTreeMap<String, CmdIdx>) -> Result<u64> {
    let mut uncompacted = 0;
    // 从所有的数据文件中加载数据到索引中
    for file_name in file_names {
      // 每个文件的reader
      let file = File::open(data_file_path(dir, file_name)?)?;
      let mut file_reader = BufReader::new(file);
      uncompacted += load_idx_from_file(file_name, &mut file_reader, index)?;
      
      // 每个文件的reader都保存下来，get的时候，根据key找到索引，索引中有文件名和key对应的位置。
      readers.insert(file_name, file_reader);
    }
  Ok(uncompacted)
}

fn load_idx_from_file(file_name: u32, 
  file_reader: &mut BufReader<File>, 
  index: &mut BTreeMap<String, CmdIdx>) -> Result<u64> {
  let mut uncompacted = 0;
  // 从文件开始位置读
  let mut start_pos = file_reader.seek(SeekFrom::Start(0))?;
  // 按Command的json格式读
  let mut from_reader = Deserializer::from_reader(file_reader).into_iter::<Command>();
  while let Some(cmd) = from_reader.next() {
    // command的结束位置
    let end_pos = from_reader.byte_offset() as u64;
    match cmd? {
      // 匹配到set命令
      Command::Set { key, .. } => {
        // 将数据的位置范围记录在Btreemap中
        let cmd_index: CmdIdx = (file_name, Range {start: start_pos, end: end_pos}).into();
        if let Some(cmd_old) = index.insert(key, cmd_index) {
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
  Ok(uncompacted)
}
```
### 记作v1
到这里，就算是第一阶段完成了，这个过程中有很大的收获，这个项目里的代码到目前为止，可以学习到大量的io操作，json序列化到文件，读取，也算是很不错的代码练习项目了。

## 网络客户端
这个章节记作v2，在这个章节中，将会给这个应用加入网络连接的功能，使用客户端可以连接到服务端，使用网络请求完成对数据的操作。  

### 加入服务端
添加一个mod，名叫server，它包含了KvStore的实例对象，并且有一个start方法，可以启动服务。
```rust
use std::{io::Result, net::{TcpListener, TcpStream}};

use crate::kv::KvStore;

const SERVER_PORT: &str = "127.0.0.1:4000";

pub struct KvServer {
  store: KvStore,
}

impl KvServer {
  pub fn new(&mut self) -> Result<KvServer> {
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
    todo!();
    Ok(())
  }
}
```
参考标准库，现在已经完成了服务端的网络监听，接下来要做的就是将请求来的数据转换成命令，然后再调用KvStore，就行了。那就需要继续在handle_connection这个方法中完成该需求。  

在开始处理handle_connection的工作前，需要定义一下统一的请求和响应，这样在服务端和客户端都可以使用一致的处理方式。  
```rust
// req.rs
use serde::{Deserialize, Serialize};

use crate::kv::command::Command;

#[derive(Debug, Deserialize, Serialize)]
pub struct Request {
  command: Command,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
  result: Result<Option<String>, String>
}
```

处理请求，handle_connection  
```rust
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

        },
        Command::Remove { key } => {

        },
      }
    }
    Ok(())
  }
```
先实现一个set方法，然后再写个测试方法，测试一下：
```rust
  #[test]
  fn test_tcp_set() -> io::Result<()> {
    let tcp_stream = TcpStream::connect("127.0.0.1:4000")?;

    let mut writer = BufWriter::new(&tcp_stream);
    let value = Command::Set { key: "key".to_string(), value: "value".to_string() };

    serde_json::to_writer(&mut writer, &Request{command: value})?;
    writer.flush()?;

    let buf_reader = BufReader::new(&tcp_stream);
    let mut reader = Deserializer::from_reader(buf_reader);
    let resp = Response::deserialize(&mut reader)?;

    assert_eq!(resp.result, Ok(Some("ok".to_string())));

    Ok(())
  }
```
测试了一下，通过了。接下来就是照葫芦画瓢，把get和remove完善一下。

```rust
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
```
还是和之前一样，需要编写一个测试方法，它可以使我们心里更加的放松。
```rust
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
```

测试结果当然是令人满意的，至此，server已经开发完了，那么，接下来就是client了。

### 编写client

因为之前已经写了测试方法了，所以，编写的client的时候，就参考测试方法就行了。
```rust
// client.rs
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
```
写过和server和测试方法后，这个client写起来就感觉没那么难了。好了，把两个端都跑起来吧。看下效果。  
启动服务端  
```shell
cargo run --bin server 
```

客户端
```shell
cargo run --bin client -- --help

Usage: client [OPTIONS] <COMMAND>

Commands:
  set     
  get     
  remove  
  help    Print this message or the help of the given subcommand(s)

Options:
  -p, --port <IP:PORT>  
  -h, --help            Print help
  -V, --version         Print version
```

客户端命令
```shell
cargo run --bin client -- set key value
Response { result: Ok(Some("ok")) }

cargo run --bin client -- get key
Response { result: Ok(Some("value")) }

cargo run --bin client -- remove key
Response { result: Ok(Some("ok")) }

cargo run --bin client -- get key
Response { result: Ok(None) }
```

## server端多线程

现在的server端的处理命令的方式是单线程的处理方式。现在我们尝试将其改造成多线程的处理方式。  

首先，需要写一个线程池，这个线程池rust官网教程是有一个章节介绍过的，多线程web应用的那个章节。我们可以参考它的多线程和线程池的实现方式。  

```rust
use std::{io::Result, sync::{mpsc::{channel, Receiver, Sender}, Arc, Mutex}, thread::{self, JoinHandle}, usize};

struct Worker {
  id: usize,
  thread: Option<JoinHandle<()>>,
}

impl Worker {
  fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>) -> Worker {
    let thread = thread::spawn(move || loop {
      match receiver.lock().unwrap().recv() {
        Ok(job) => {
          println!("woker {id} 接收到一个任务，开始执行。");
          job();
        },
        Err(_) => println!("worker {id} 断开了连接，正在关闭。"),
      }
    });
    Worker { id, thread: Some(thread), }
  }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
  workers: Vec<Worker>,
  sender: Option<Sender<Job>>,
}

impl ThreadPool {
  pub fn new(size: usize) -> Result<ThreadPool> {
    assert!(size > 0);
    println!("开始创建线程池，线程数量：{size}");

    let (sender, receiver) = channel::<Job>();
    let receiver = Arc::new(Mutex::new(receiver));

    let mut workers = Vec::with_capacity(size);
    for i in 0..size {
      let worker = Worker::new(i, Arc::clone(&receiver));
      workers.push(worker);
      println!("第 {i} 个线程已创建, 线程id: {i}");
    }

    println!("线程池创建完毕");
    Ok(ThreadPool {workers, sender: Some(sender)})
  }

  pub fn execute<F>(&self, f: F)
  where 
    F: FnOnce() + Send + 'static, 
  {
    println!("线程池推入任务。");
    self.sender.as_ref().unwrap().send(Box::new(f)).unwrap();
  }
}

impl Drop for ThreadPool {
  fn drop(&mut self) {
    drop(self.sender.take());
    for worker in &mut self.workers {
      println!("Shutting down worker {}", worker.id);
      if let Some(thread) = worker.thread.take() {
        thread.join().unwrap();
      }
    }
  }
}
```
这个线程池应该是不用多说了，官网教程上一样一样的。

我们新建一个支持多线程的store,名字就叫`AsynKvStore`，它的属性是这样的：
```rust
pub struct AsynKvStore {
  writer: Arc<Mutex<StoreWriter>>,
  reader: StoreReader,
  index: Arc<Mutex<BTreeMap<String, CmdIdx>>>
}
```
这里和之前的单线程的不太一样，这里目的是期望使用`reader`和`writer`来将读操作和写操作独立开。get方法属于读操作，就把它的逻辑写的reader中，set和remove方法是写操作，就把它们两个写在writer，因为是多线程操作，writer是要加锁的，这样的话就不太会影响到reader的操作。  
并且这里也很多属性都使用到了`Arc`这个东西，它是原子引用记数，方便的多线程的场景中对数据进行引用。  

StoreReader中放入每个文件的reader，使用到了RefCell来标记readers所在的HashMap，这个RefCell是一个可以内部存储的容器，我是这样理解的，数据对象整体不变，但是内部局部可变，和Cell有着一样的定义，但是两个又有区别，Cell适用于可Copy的小数据结构，RefCell则是适用于非Copy的大对象数据结构，并且提供引用。  

`safe_point`这个字段，可以看到这里定义了一个全局的引用类型，它主要的作用就是用来记录数据文件压缩完成后写入的压缩文件，然后清除文件时，这个节点前的文件就都可以清除，hashMap中的保存的BufReader也可以根据这个字段来清除，小于`safe_point`的reader全部清除掉。

```rust
struct StoreReader {
  readers: RefCell<HashMap<u32, BufReader<File>>>,
  data_path: Arc<PathBuf>,
  safe_point: Arc<AtomicU32>
}
```

清除reader的操作：
```rust
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
```

reader负责`get`，writer负责`set`和`remove`：

```rust
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
```


在写这个的时候，被rust的借用检查快给整疯了，好在静下心来写了好多demo，自己验证了很多rust语言的借用方式，最终才坚持写下来的。  

HashMap没有实现Clone，所以这里的StoreReader实现了Clone，其它地方的引用StoreReader的时候，可以通过这个clone方法，将其中的readers单独构建出来一份，这样的好处在于，各个线程或其它地方的readers中的BufReader不是共享的，之前不存在引用关系，所以文件读取的时候，或许会有到seek的操作，这样就不会相互影响，至少我是这样理解的，我也没有试多个线程使用引用的时候，seek会不会相互，我想应该会影响吧，哈哈。  

```rust
impl Clone for StoreReader {
  fn clone(&self) -> Self {
    Self { 
      readers: RefCell::new(HashMap::new()),
      data_path: Arc::clone(&self.data_path),
      safe_point: Arc::clone(&self.safe_point),
    }
  }
}
```

然后就是正常的写AsynStore的`open`方法，这样，这样，然后再这样就好了：
```rust
  pub fn open() -> Result<Self> {
    let data_path = data_dir()?;
    fs::create_dir_all(&data_path)?;

    let mut index: BTreeMap<String, CmdIdx> = BTreeMap::new();
    let mut readers: HashMap<u32, BufReader<File>> = HashMap::new();
    let mut uncompacted = 0u64;

    let file_names = sorted_file_names(&data_path)?;
    let cur_data_file_name = file_names.last().unwrap_or(&0)+1;
    let data_files = file_names.clone();
    uncompacted += load_idx(&data_path, file_names, &mut readers, &mut index)?;

    let index = Arc::new(Mutex::new(index));
    let writer = StoreWriter {
      cur_data_file_name,
      uncompacted,
      index: index.clone(),
      writer: new_data_file(&data_path, cur_data_file_name, &mut readers)?,
    }; 

    let data_path = Arc::new(data_path);
    let reader = StoreReader {
      readers: RefCell::new(readers),
      data_files: Arc::new(data_files),
      data_path: Arc::clone(&data_path),
    };

    let store = AsynKvStore {
      writer: Arc::new(Mutex::new(writer)),
      reader,
      index,
    };
    Ok(store)
  }

```

get方法还是和原来的大差不差：
```rust
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
}
```

接下来就是把`set`和`remove`方法补充一下：
```rust
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
```

基本逻辑就是和单线程的操作是一样的，只不过是加入了线程共享变量，由于语言的特性，不了解的情况下比较难处理。  

最后要操作的就是compact了：
```rust
n compact(&mut self) -> Result<()> {
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
```
它的逻辑就是根据`index`是的位置数据，从文件中读出指令，将它们写入新的文件中，然后清除旧文件。