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
    // max of 64 bytes words
    let mut idx: usize = 0;
    const ACC_LEN: usize = 32;
    let mut echo_flag: bool = false;
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
            // we have a full instruction!
            let msg: &[u8] = &accumulator[..idx];

            if msg.starts_with(b"$") {
                println!("Ignoring lengths for the time being");
            } else {
                match msg {
                    b"PING\r\n" => {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                    b"ECHO\r\n" => {
                        echo_flag = true;
                    }
                    val if echo_flag => {
                        stream.write_all(b"+").unwrap();
                        stream.write_all(val).unwrap();
                        echo_flag = false;
                    }
                    other => {
                        println!("Unhandled for the moment: {:?}", other);
                    }
                }
            }
            
            accumulator.fill(0);
            idx = 0;
        }
    }
}