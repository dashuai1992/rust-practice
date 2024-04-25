
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

知行合一，万事开头难，那就边做边想吧。  

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

struct KvStore {

}

impl KvStore {
    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        
        Ok(())
    }
}
```
