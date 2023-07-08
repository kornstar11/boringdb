use parking_lot::Mutex;
use redis_protocol::resp2::prelude::*;
use bytes::{Bytes, BytesMut};
use std::default;
use std::io::prelude::*;
use std::net::{TcpStream, SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use std::thread::{spawn, JoinHandle};
use crate::{Database, DatabaseContext};
use crate::error::*;

// fn main() -> std::io::Result<()> {
//     let mut stream = TcpStream::connect("127.0.0.1:34254")?;

//     stream.write(&[1])?;
//     stream.read(&mut [0; 128])?;
//     Ok(())
// }

enum DatabaseCommands {
    Get{key: Vec<u8>, cb: SyncSender<Result<Option<Vec<u8>>>>},
    Put{key: Vec<u8>, value: Vec<u8>, cb: SyncSender<Result<()>>}
}

impl DatabaseCommands {
    fn get(key: Vec<u8>) -> (DatabaseCommands, Receiver<Result<Option<Vec<u8>>>>) {
        let (cb, rx) = sync_channel(1);
        let cmd = Self::Get{key, cb};
        (cmd, rx)
    }

    fn put(key: Vec<u8>, value: Vec<u8>) -> (DatabaseCommands, Receiver<Result<()>>) {
        let (cb, rx) = sync_channel(1);
        let cmd = Self::Put{key, value, cb};
        (cmd, rx)
    }
}

//6379
struct ServerState {
    db: Arc<Mutex<Database>>
}

impl ServerState {
    fn new() -> Self {
        let db = Database::start(DatabaseContext::default());
        Self {
            db
        }
    }
    
}

struct ServerFactory {
    addr: SocketAddr,
    outstanding_requests: usize,
}

impl ServerFactory {
    pub fn start(&self) -> JoinHandle<()> {
        let (tx, rx) = sync_channel::<DatabaseCommands>(self.outstanding_requests);


        let forwarder = spawn(move || {
            let mut state = ServerState::new();
            while let Ok(cmd) = rx.recv() {
                match cmd {
                    DatabaseCommands::Get { key, cb } => {
                        if let Err(_) = cb.send(state.db.lock().get(key.as_ref())) {
                            log::warn!("Callback dead.");
                            return;
                        }
                    },
                    DatabaseCommands::Put { key, value, cb } => {
                        if let Err(_) = cb.send(state.db.lock().put(key, value)) {
                            log::warn!("Callback dead.");
                            return;
                        }
                    }
                }
            }
            log::info!("Stopping network thread (db)");
        });

        let network_thread = spawn(move || {
            match TcpListener::bind(self.addr) {
                Ok(tcp) => {
                    while let Ok((stream, _remote)) = tcp.accept() {


                    }

                },
                Err(e) => {
                    log::error!("Unable to bind, due to: {:?}", e);
                    return;
                }
            }
        });
    }

    fn handler(mut stream: TcpStream) {
        let mut outer_buf = vec![0 as usize; 1024];
        let mut buf = [0 as u8; 1024];
        while let Ok(bytes_read) = stream.read(&mut buf) {
            if bytes_read == 0 {
                log::info!("Closed connection.")
                return;
            }

        }

        // while let Ok(frame_opt) = decode(stream.read(&mut buf)) {
        //     match frame_opt {
        //         Some((frame, _))
        //     }
        // }

        // let (frame, consumed) = match decode(&buf) {
        //     Ok(Some((f, c))) => (f, c),
        //     Ok(None) => panic!("Incomplete frame."),
        //     Err(e) => panic!("Error parsing bytes: {:?}", e)
        //   };
    }
    
}
