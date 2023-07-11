use parking_lot::Mutex;
use redis_protocol::resp2::prelude::*;
use bytes::{Bytes, BytesMut};
use redis_protocol::resp3::encode::complete::encode;
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

// enum DatabaseCommands {
//     Get{key: Vec<u8>, cb: SyncSender<Result<Option<Vec<u8>>>>},
//     Put{key: Vec<u8>, value: Vec<u8>, cb: SyncSender<Result<()>>}
// }

// impl DatabaseCommands {
//     fn get(key: Vec<u8>) -> (DatabaseCommands, Receiver<Result<Option<Vec<u8>>>>) {
//         let (cb, rx) = sync_channel(1);
//         let cmd = Self::Get{key, cb};
//         (cmd, rx)
//     }

//     fn put(key: Vec<u8>, value: Vec<u8>) -> (DatabaseCommands, Receiver<Result<()>>) {
//         let (cb, rx) = sync_channel(1);
//         let cmd = Self::Put{key, value, cb};
//         (cmd, rx)
//     }
// }

struct FrameWithCallback {
    frame: Frame,
    cb: SyncSender<Frame>
}

impl FrameWithCallback {
    fn split(self) -> (Frame, SyncSender<Frame>) {
        (self.frame, self.cb)
    }

    fn new(frame: Frame) -> (Self, Receiver<Frame>) {
        let (cb, rx) = sync_channel(1);
        (Self{frame, cb}, rx)
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

#[derive(Clone)]
struct ServerFactory {
    addr: SocketAddr,
    outstanding_requests: usize,
}

impl ServerFactory {
    pub fn start(&self) -> JoinHandle<()> {
        let (tx, rx) = sync_channel::<FrameWithCallback>(self.outstanding_requests);


        let forwarder = spawn(move || {
            let mut state = ServerState::new();
            while let Ok(cmd) = rx.recv() {
                let (frame, cb) = cmd.split();

                    // DatabaseCommands::Get { key, cb } => {
                    //     if let Err(_) = cb.send(state.db.lock().get(key.as_ref())) {
                    //         log::warn!("Callback dead.");
                    //         return;
                    //     }
                    // },
                    // DatabaseCommands::Put { key, value, cb } => {
                    //     if let Err(_) = cb.send(state.db.lock().put(key, value)) {
                    //         log::warn!("Callback dead.");
                    //         return;
                    //     }
                    // }
            }
            log::info!("Stopping network thread (db)");
        });
        let network_self = self.clone();
        let network_thread = spawn(move || {
            match TcpListener::bind(network_self.addr) {
                Ok(tcp) => {
                    while let Ok((stream, _remote)) = tcp.accept() {
                        if let Err(e) = Self::handler(stream, tx.clone()) {
                            log::warn!("Client error: {:?}", e);
                        }
                    }

                },
                Err(e) => {
                    log::error!("Unable to bind, due to: {:?}", e);
                    return;
                }
            }
        });
        todo!()
    }

    fn handler(mut stream: TcpStream, tx_commands: SyncSender<FrameWithCallback>) -> Result<()> {
        let mut outer_buf = BytesMut::new();
        let mut buf = BytesMut::with_capacity(1024);//[0 as u8; 1024];
        while let Ok(bytes_read) = stream.read(&mut buf) {
            if bytes_read == 0 {
                log::info!("Closed connection.");
                return Ok(());
            }
            //let bytes = buf.clone().freeze();
            match decode_mut(&mut buf) {
                Ok(Some((frame, _read, _consumed))) => {
                    let (frame_with_cb, cb) = FrameWithCallback::new(frame);
                    if let Err(_) = tx_commands.send(frame_with_cb) {
                        log::info!("Recv loop closed.");
                        return Ok(());
                    }
                    if let Ok(resp) = cb.recv() {
                        let encode = encode(buf, offset, frame)

                    }
                    
                },
                Ok(None) if outer_buf.len() <= 1000_000 => {
                    // not enough bytes so save it off
                    outer_buf.extend_from_slice(buf.as_ref())
                },
                Ok(None) => {
                    return Err(Error::Other(String::from("Unable to make a frame, since we exceeded the max_bytes")));
                },
                Err(e) => {
                    return Err(Error::Redis(e));
                }
            }
        }

        todo!()

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
