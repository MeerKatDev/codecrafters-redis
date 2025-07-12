#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Read, Write};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let mut buf = [0; 128];

                while let Ok(read_count) = stream.read(&mut buf) {
                    if read_count == 0 {
                        break;
                    }

                    println!("Received: {:?}", std::str::from_utf8(&buf[..read_count]));
                    
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
