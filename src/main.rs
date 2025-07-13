#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

mod redis_instruction;
use crate::redis_instruction::{ExpiryMsg, Instruction, Storage};

fn main() {
  let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
  let store = Arc::new(Mutex::new(HashMap::new()));
  let (tx, rx) = mpsc::channel();
  spawn_expiry_handler(Arc::clone(&store), rx);

  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        println!("accepted new connection");
        let store = Arc::clone(&store);
        let tx_clone = tx.clone();
        thread::spawn(move || handle_connection(stream, store, tx_clone));
      }
      Err(e) => {
        println!("error: {}", e);
      }
    }
  }
}

fn handle_connection(
  mut stream: TcpStream,
  store: Arc<Mutex<Storage>>,
  tx: mpsc::Sender<ExpiryMsg>,
) {
  let mut byte = [0u8; 1];
  // max of 64 bytes words
  let mut idx: usize = 0;
  const ACC_LEN: usize = 32;
  let mut accumulator = [0u8; ACC_LEN];
  let mut instr = Instruction::new(store.clone(), tx);
  instr.save_store_ref(store);

  while let Ok(()) = stream.read_exact(&mut byte) {
    // simply accumulate and deal with overflow
    if idx < accumulator.len() {
      accumulator[idx] = byte[0];
      idx += 1;
    } else {
      panic!("Buffer full, instructions too large!");
    }

    if accumulator[..idx].ends_with(b"\r\n") {
      let msg = std::str::from_utf8(&accumulator[..idx])
        .unwrap()
        .to_string();

      if msg.starts_with("*") {
        instr.parse_args_length(&msg);
      } else if msg.starts_with("$") {
        println!("Ignore length")
      } else {
        println!("Execute {}", msg);
        if instr.name.is_none() {
          instr.parse_command(&msg);
        } else {
          instr.parse_argument(&msg);
        }
      }

      stream.write_all(&instr.make_response()).unwrap();

      idx = 0;
      accumulator.fill(0);
    }
  }
}

fn spawn_expiry_handler(store: Arc<Mutex<Storage>>, rx: mpsc::Receiver<ExpiryMsg>) {
  thread::spawn(move || {
    while let Ok((key, ms)) = rx.recv() {
      thread::sleep(Duration::from_millis(ms));
      let mut store = store.lock().unwrap();
      store.remove(&key);
      println!("Expired and removed key: {}", key);
    }
  });
}
