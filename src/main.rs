#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

mod redis_instruction;
use crate::redis_instruction::{ExpiryMsg, Instruction, Storage};

fn main() {
  let config = Arc::new(Mutex::new(get_config()));

  let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
  // let mut storage_map: Storage = HashMap::new();
  // storage_map.insert("config".to_owned(), config);
  let storage = Arc::new(Mutex::new(HashMap::new()));
  let (tx, rx) = mpsc::channel();
  spawn_expiry_handler(Arc::clone(&storage), rx);

  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        println!("accepted new connection");
        let tx_clone = tx.clone();
        let config_clone = Arc::clone(&config);
        let storage_clone = Arc::clone(&storage);
        thread::spawn(move || handle_connection(stream, config_clone, storage_clone, tx_clone));
      }
      Err(e) => {
        println!("error: {}", e);
      }
    }
  }
}

fn handle_connection(
  mut stream: TcpStream,
  config: Arc<Mutex<Storage>>,
  storage: Arc<Mutex<Storage>>,
  tx: mpsc::Sender<ExpiryMsg>,
) {
  // let mut reader = BufReader::new(&mut stream);
  let mut instr = Instruction::new(config, storage, tx);
  let mut byte = [0u8; 1];
  let mut buffer = Vec::new();
  // while reader.read_line(&mut line).unwrap() > 0 {
  while stream.read_exact(&mut byte).is_ok() {
    buffer.push(byte[0]);
    // Trim trailing newline and carriage return
    if !buffer.ends_with(b"\r\n") {
      continue;
    }

    let msg = String::from_utf8_lossy(&buffer)
      .trim_end_matches(&['\r', '\n'][..])
      .to_string();

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

    if instr.is_ready() {
      stream.write_all(&instr.make_response()).unwrap();
      instr.clear();
    }

    buffer.clear();
  }
}

fn get_config() -> HashMap<String, String> {
  let mut args_map = HashMap::new();
  let mut args = env::args().skip(1);

  while let Some(arg) = args.next() {
    if arg.starts_with("--") {
      if let Some(value) = args.next() {
        args_map.insert(arg.trim_start_matches('-').to_string(), value);
      } else {
        eprintln!("Expected value after {}", arg);
        return Default::default();
      }
    } else {
      eprintln!("Ignoring unexpected argument {}", arg);
    }
  }
  args_map
}

fn spawn_expiry_handler(storage: Arc<Mutex<Storage>>, rx: mpsc::Receiver<ExpiryMsg>) {
  thread::spawn(move || {
    while let Ok((key, ms)) = rx.recv() {
      thread::sleep(Duration::from_millis(ms));
      let mut storage = storage.lock().unwrap();
      storage.remove(&key);
      println!("Expired and removed key: {}", key);
    }
  });
}
