use crate::error::*;
use crate::{Database, DatabaseContext};
use bytes::{Bytes, BytesMut, Buf};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use redis_protocol::resp2::prelude::*;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::spawn;

static UNKNOWN_RESP: Lazy<Frame> = Lazy::new(|| Frame::Error("Unknown command".into()));

static OK_FRAME: Lazy<Frame> = Lazy::new(|| Frame::BulkString("OK".into()));

struct FrameWithCallback {
    frame: Frame,
    cb: SyncSender<Frame>,
}

impl FrameWithCallback {
    fn split(self) -> (Frame, SyncSender<Frame>) {
        (self.frame, self.cb)
    }

    fn new(frame: Frame) -> (Self, Receiver<Frame>) {
        let (cb, rx) = sync_channel(1);
        (Self { frame, cb }, rx)
    }
}

//6379
struct ServerState {
    db: Arc<Mutex<Database>>,
}

impl ServerState {
    fn new() -> Self {
        let db = Database::start(DatabaseContext::default());
        Self { db }
    }

    fn decode_state(&self, frame: Frame) -> Frame {
        log::trace!("RESP Frame: {:?}", frame);
        match frame {
            Frame::Array(frames) => {
                if let Some((Frame::BulkString(cmd), args)) = frames.split_first() {
                    match cmd.as_ref() {
                        b"DEL" => {
                            if let [Frame::BulkString(key_bytes)] = args {
                                match self.db.lock().delete(&key_bytes) {
                                    Ok(()) => {
                                        return OK_FRAME.clone();
                                    }
                                    Err(e) => {
                                        log::warn!("DB DEL error: {}", e.to_string());
                                        return Frame::Error(e.to_string().into());
                                    }
                                }
                            }
                        }
                        b"GET" => {
                            if let [Frame::BulkString(key_bytes)] = args {
                                match self.db.lock().get(&key_bytes) {
                                    Ok(Some(v)) => {
                                        return Frame::BulkString(Bytes::copy_from_slice(&v));
                                    }
                                    Ok(None) => {
                                        return Frame::Null;
                                    }
                                    Err(e) => {
                                        log::warn!("DB GET Error: {}", e.to_string());
                                        return Frame::Error(e.to_string().into());
                                    }
                                }
                            }
                        }
                        b"SET" => {
                            if let [Frame::BulkString(key_bytes), Frame::BulkString(value_bytes)] =
                                args
                            {
                                match self.db.lock().put(key_bytes.to_vec(), value_bytes.to_vec()) {
                                    Ok(()) => {
                                        return OK_FRAME.clone();
                                    }
                                    Err(e) => {
                                        log::warn!("Client error durring put: {}", e.to_string());
                                        return Frame::Error(e.to_string().into());
                                    }
                                }
                            }
                        }
                        b"FLUSHALL" => {
                            match self.db.lock().flush_to_disk() {
                                Ok(()) => {
                                    return OK_FRAME.clone();
                                }
                                Err(e) => {
                                    log::warn!("Client error durring put: {}", e.to_string());
                                    return Frame::Error(e.to_string().into());
                                }
                            }
                        }
                        // b"CONFIG" => {
                        //     if let [Frame::BulkString(_get_set), Frame::BulkString(key)] = args {
                        //         return Frame::Array(vec![Frame::BulkString(key.clone()), Frame::BulkString("3600 1 300 100 60 10000".into())]);
                        //     }
                        // }
                        _ => {}
                    }
                }
            }
            _ => {}
        };

        UNKNOWN_RESP.clone()
    }
}

#[derive(Clone)]
pub struct ServerFactory {
    pub addr: SocketAddr,
    pub outstanding_requests: usize,
}

impl ServerFactory {
    pub fn start(&self) -> Result<()> {
        let (tx, rx) = sync_channel::<FrameWithCallback>(self.outstanding_requests);

        let forwarder_thread = spawn(move || {
            let mut state = ServerState::new();
            while let Ok(cmd) = rx.recv() {
                let (frame, cb) = cmd.split();
                if let Err(_) = cb.send(state.decode_state(frame)) {
                    break;
                }
            }
            log::info!("Stopping network thread (db)");
        });
        let network_self = self.clone();
        let network_thread = spawn(move || {
            log::info!("Server binding to TCP {:?}", network_self.addr);
            match TcpListener::bind(network_self.addr) {
                Ok(tcp) => {
                    log::info!("Accepting connections");
                    while let Ok((stream, remote)) = tcp.accept() {
                        log::info!("Accepting connection from {:?}", remote);
                        if let Err(e) = Self::handler(stream, tx.clone()) {
                            log::warn!("Client error: {:?}", e);
                        }
                        log::info!("Closed connection.");
                    }
                }
                Err(e) => {
                    log::error!("Unable to bind, due to: {:?}", e);
                    return;
                }
            }
        });

        forwarder_thread
            .join()
            .map_err(|_| Error::Other(String::from("Unable to join forwarder thread.")))?;
        network_thread
            .join()
            .map_err(|_| Error::Other(String::from("Unable to join network thread.")))?;
        Ok(())
    }

    fn handler(mut stream: TcpStream, tx_commands: SyncSender<FrameWithCallback>) -> Result<()> {
        let cap = 1024;
        let mut pos = 0;
        let mut outer_buf = BytesMut::zeroed(cap);
        //let mut buf = vec![0 as u8; 1024]; 
        let mut send_buf = BytesMut::zeroed(cap); //[0 as u8; 1024];
        while let Ok(bytes_read) = stream.read(&mut outer_buf[pos..]) {
            //log::trace!("Bytes read == {}", bytes_read);
            if bytes_read == 0 {
                return Ok(());
            }
            pos += bytes_read;
            //outer_buf.extend_from_slice(&buf[0..bytes_read]);

            //let bytes = buf.clone().freeze();
            match decode_mut(&mut outer_buf) {
                Ok(Some((frame, _read, _consumed))) => {
                    //outer_buf.resize(cap, 0);
                    //if outer_buf.len() == 0 {
                    outer_buf.reserve(cap);
                    unsafe {outer_buf.set_len(cap);}
                    //}
                    pos = 0;

                    let (frame_with_cb, cb) = FrameWithCallback::new(frame);
                    if let Err(_) = tx_commands.send(frame_with_cb) {
                        log::debug!("Recv loop closed.");
                        return Ok(());
                    }
                    if let Ok(resp) = cb.recv() {
                        send_buf.clear();
                        //log::debug!("Sending back: {:?}", resp);
                        let encode_len =
                            encode_bytes(&mut send_buf, &resp).map_err(Error::Redis)?;
                        //log::debug!("Encoded bytes to send: {}", encode_len);
                        stream.write(&send_buf[0..encode_len])?;
                    }
                }
                Ok(None) if outer_buf.len() <= 1_000_000 => {
                    //outer_buf.advance(bytes_read);
                    // not enough bytes so save it off
                    outer_buf.resize(outer_buf.len() + cap, 0);
                    //log::trace!("Buffering...");
                    // outer_buf.extend_from_slice(buf.as_ref())
                }
                Ok(None) => {
                    return Err(Error::Other(String::from(
                        "Unable to make a frame, since we exceeded the max_bytes",
                    )));
                }
                Err(e) => {
                    log::error!("While reading, {:?}", e);
                    return Err(Error::Redis(e));
                }
            }
        }

        Ok(())
    }
}
