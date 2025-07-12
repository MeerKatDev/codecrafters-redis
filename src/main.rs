#![allow(unused_imports)]
use std::net::{TcpStream, TcpListener};
use std::io::{Read, Write};
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");

                thread::spawn(move || {
                    handle_connection(stream)
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}


fn handle_connection(mut stream: TcpStream) {
    let mut byte = [0u8; 1];

    while let Ok(()) = stream.read_exact(&mut byte) {
        let b = byte[0];

        println!("Got byte: {}", b as char);
        
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}