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