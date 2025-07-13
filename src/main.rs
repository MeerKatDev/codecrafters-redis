#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

type Storage = HashMap<String, String>;
type ExpiryMsg = (String, u64);

// pub struct Instruction {
//   len: u8,
//   name: String,
//   arguments: [0;]
// }

fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  println!("Logs from your program will appear here!");

  // Uncomment this block to pass the first stage
  //
  let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
  let store = Arc::new(Mutex::new(HashMap::new()));
  let (tx, rx) = mpsc::channel();
  spawn_expiry_handler(Arc::clone(&store), rx);

  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        println!("accepted new connection");

        let store = Arc::clone(&store);

        let tx = tx.clone();
        thread::spawn(move || handle_connection(stream, store, tx));
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
  let mut instruction: &str = "";
  // let mut instr = Vec::new();
  let mut instr_len: u8 = 0;
  let mut first_arg: String = "".to_string();
  let mut second_arg: String = "".to_string();
  let mut third_arg: String = "".to_string();
  let mut accumulator = [0u8; ACC_LEN];

  while let Ok(()) = stream.read_exact(&mut byte) {
    // simply accumulate and deal with overflow
    if idx < accumulator.len() {
      accumulator[idx] = byte[0];
      idx += 1;
    } else {
      panic!("Buffer full, instructions too large!");
    }

    let msg = std::str::from_utf8(&accumulator[..idx])
      .unwrap()
      .to_string();

    // read the instruction
    if msg.ends_with("\r\n") {
      if msg.starts_with("*") {
        let number_str = &msg[1..msg.len() - 2]; // skip '*' and trailing "\r\n"
        instr_len = number_str.parse::<u8>().unwrap()
      } else if msg.starts_with("$") {
        println!("Ignore this")
      } else {
        println!("Execute {}", msg);
        match msg.as_str() {
          "PING\r\n" => {
            stream.write_all(b"+PONG\r\n").unwrap();
          }
          "ECHO\r\n" => {
            instruction = "ECHO";
          }
          "GET\r\n" => {
            instruction = "GET";
          }
          "SET\r\n" => {
            instruction = "SET";
          }
          val if instruction == "ECHO" => {
            stream.write_all(b"+").unwrap();
            stream.write_all(val.as_bytes()).unwrap();
            instruction = "";
          }
          key if instruction == "GET" => {
            let tmp_store = store.lock().unwrap();
            match tmp_store.get(key) {
              Some(val) => {
                println!("Getting {}", val);
                stream.write_all(b"+").unwrap();
                stream.write_all(val.as_bytes()).unwrap()
              }
              None => {
                println!("NOT Getting anything to {}", key);
                stream.write_all(b"$-1\r\n").unwrap()
              }
            }
            instruction = "";
          }
          "px\r\n" if instruction == "SET" => {
            third_arg = "px\r\n".to_string();
          }
          val if instruction == "SET" => {
            if first_arg.is_empty() {
              first_arg = val.to_string()
            } else if second_arg.is_empty() {
              let mut tmp_store = store.lock().unwrap();
              tmp_store.insert(first_arg.to_string(), val.to_string());
              if instr_len == 3 {
                instruction = "";
                first_arg = "".to_string();
                // terminate instruction execution
                stream.write_all(b"+OK\r\n").unwrap();
              } else {
                second_arg = val.to_string();
              }
            } else if &third_arg[..] == "px\r\n" {
              // px, key, time
              println!("{}", val.trim());
              let duration_ms = val.trim().parse::<u64>().expect("Invalid duration");
              tx.send((first_arg, duration_ms)).unwrap();
              instruction = "";
              first_arg = "".to_string();
              second_arg = "".to_string();
              third_arg = "".to_string();
              // terminate instruction execution
              stream.write_all(b"+OK\r\n").unwrap();
            }
          }
          other => {
            println!("Unhandled for the moment: {:?}", other);
          }
        }
      }

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
