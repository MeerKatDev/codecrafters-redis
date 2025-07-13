#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
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

fn handle_connection(stream: TcpStream, store: Arc<Mutex<Storage>>, tx: mpsc::Sender<ExpiryMsg>) {
  let mut reader = BufReader::new(stream);
  let mut instr = Instruction::new(store.clone(), tx);

  let mut line = String::new();
  while reader.read_line(&mut line).unwrap() > 0 {
    // Trim trailing newline and carriage return
    let msg = line.trim_end_matches(&['\r', '\n'][..]).to_string();

    match msg.chars().next() {
      Some('*') => {
        instr.parse_args_length(&msg);
      }
      Some('$') => { /* Ignore lengths */ }
      _ if instr.name.is_none() => {
        instr.parse_command(&msg);
      }
      _ => {
        instr.parse_argument(&msg);
      }
    }

    reader.get_mut().write_all(&instr.make_response()).unwrap();

    line.clear();
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
