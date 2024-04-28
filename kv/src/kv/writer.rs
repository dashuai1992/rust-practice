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

#[cfg(test)]
mod tests {
    use std::{env::current_dir, fs::File, io::{self, Seek}};

  #[test]
  fn test_seek() -> io::Result<()> {
    let mut file = File::open(current_dir()?.join("data.log"))?;
    let seek_end = file.seek(io::SeekFrom::End(0))?;
    let seek_start = file.seek(io::SeekFrom::Start(0))?;
    let seek_cur = file.seek(io::SeekFrom::Current(0))?;

    println!("seek start: {}, end: {}, current: {}", seek_start, seek_end, seek_cur);

    Ok(())
  }
}

