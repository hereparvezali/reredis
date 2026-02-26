pub mod commands;
pub mod parser;
pub mod storage;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::commands::{Command, encode_resp, execute};
use crate::parser::{Resp, parse};
use crate::storage::Storage;

#[tokio::main]
async fn main() {
    let storage = Arc::new(Storage::new());

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("ReRedis server listening on 127.0.0.1:6379");

    // Spawn a background task to periodically clean up expired keys
    let cleanup_storage = Arc::clone(&storage);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            cleanup_storage.run_expiry_cleanup();
        }
    });

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New connection from: {}", addr);
                let client_storage = Arc::clone(&storage);
                tokio::spawn(async move {
                    handle_client(stream, client_storage).await;
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: tokio::net::TcpStream, storage: Arc<Storage>) {
    let mut buffer = vec![0u8; 65536];
    let mut accumulated = Vec::new();

    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                // Connection closed
                break;
            }
            Ok(n) => {
                accumulated.extend_from_slice(&buffer[..n]);

                // Process all complete commands in the buffer
                loop {
                    if accumulated.is_empty() {
                        break;
                    }

                    match parse(&accumulated) {
                        Ok((resp, consumed)) => {
                            // Remove consumed bytes from buffer
                            accumulated.drain(..consumed);

                            // Execute the command
                            let response = match Command::from_resp(&resp) {
                                Ok(cmd) => {
                                    // Handle QUIT command specially
                                    if cmd.name == "QUIT" {
                                        let resp = encode_resp(&Resp::Simple("OK".to_string()));
                                        let _ = stream.write_all(&resp).await;
                                        return;
                                    }
                                    execute(&cmd, &storage)
                                }
                                Err(e) => Resp::Error(e),
                            };

                            // Encode and send response
                            let encoded = encode_resp(&response);
                            if let Err(e) = stream.write_all(&encoded).await {
                                eprintln!("Failed to write response: {}", e);
                                return;
                            }
                        }
                        Err(_) => {
                            // Incomplete data, wait for more
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading from socket: {}", e);
                break;
            }
        }
    }
}
