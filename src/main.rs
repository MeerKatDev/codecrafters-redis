#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  println!("Logs from your program will appear here!");

  // Uncomment this block to pass the first stage
  //
  let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
  // let mut store = HashMap::new();
  let store = Arc::new(Mutex::new(HashMap::new()));

  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        println!("accepted new connection");

        let store = Arc::clone(&store);

        thread::spawn(move || handle_connection(stream, store));
      }
      Err(e) => {
        println!("error: {}", e);
      }
    }
  }
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<HashMap<String, String>>>) {
  let mut byte = [0u8; 1];
  // max of 64 bytes words
  let mut idx: usize = 0;
  const ACC_LEN: usize = 32;
  let mut instruction: &str = "";
  let mut first_arg: String = "".to_string();
  let mut accumulator = [0u8; ACC_LEN];

  while let Ok(()) = stream.read_exact(&mut byte) {
    // simply accumulate and deal with overflow
    if idx < accumulator.len() {
      accumulator[idx] = byte[0];
      idx += 1;
    } else {
      panic!("Buffer full, instructions too large!");
    }

    // read the instruction
    if accumulator[..idx].ends_with(b"\r\n") {
      if accumulator[..idx].starts_with(b"$") {
        println!("Ignoring lengths for the time being");
      } else {
        let msg = std::str::from_utf8(&accumulator[..idx])
          .unwrap()
          .to_string();

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
            stream.write_all(b"+").unwrap();

            let tmp_store = store.lock().unwrap();
            match tmp_store.get(key) {
              Some(val) => stream.write_all(val.as_bytes()).unwrap(),
              None => stream.write_all(b"$-1\r\n").unwrap(),
            }
            instruction = "";
          }
          key if instruction == "SET" && first_arg.is_empty() => first_arg = key.to_string(),
          val if instruction == "SET" => {
            let mut tmp_store = store.lock().unwrap();
            tmp_store.insert(first_arg.to_string(), val.to_string());
            stream.write_all(b"+OK\r\n").unwrap();
            instruction = "";
            first_arg = "".to_string();
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
