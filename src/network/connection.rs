// src/network/connection.rs
use anyhow::Error;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::frame::{Frame, MAX_FRAME_BYTES};

#[derive(Clone)]
pub struct SharedWriter {
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

impl SharedWriter {
    fn new(writer: OwnedWriteHalf) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub async fn write_bytes(&self, bytes: Vec<u8>) {
        let mut writer = self.writer.lock().await;
        if let Err(e) = writer.write_all(&bytes).await {
            log::error!("Failed to write to socket; err = {:?}", e);
        }
    }
}

enum Writer {
    Direct(OwnedWriteHalf),
    Shared(SharedWriter),
}

pub struct Connection {
    reader: OwnedReadHalf,
    read_buf: Vec<u8>,
    writer: Option<Writer>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        if let Err(e) = stream.set_nodelay(true) {
            log::warn!("Failed to set TCP_NODELAY; err = {:?}", e);
        }
        let (reader, writer) = stream.into_split();
        Connection {
            reader,
            read_buf: Vec::new(),
            writer: Some(Writer::Direct(writer)),
        }
    }

    pub async fn read_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let mut temp_bytes: [u8; 1024] = [0; 1024];

        loop {
            if self.read_buf.len() > MAX_FRAME_BYTES {
                return Err(Error::msg("ERR protocol frame exceeds configured limit"));
            }
            let complete_len = Frame::complete_frames_len(&self.read_buf);
            if complete_len > 0 {
                let bytes = self.read_buf.drain(..complete_len).collect();
                return Ok(bytes);
            }

            let n = match self.reader.read(&mut temp_bytes).await {
                Ok(n) => n,
                Err(e) => {
                    return Err(Error::msg(format!("Failed to read from stream: {:?}", e)));
                }
            };

            if n == 0 {
                if self.read_buf.is_empty() {
                    return Err(Error::msg("Connection closed by peer"));
                } else {
                    let bytes = std::mem::take(&mut self.read_buf);
                    return Ok(bytes);
                }
            }
            if self.read_buf.is_empty() {
                let complete_len = Frame::complete_frames_len(&temp_bytes[..n]);
                if complete_len > 0 {
                    if complete_len < n {
                        if self.read_buf.len() + (n - complete_len) > MAX_FRAME_BYTES {
                            return Err(Error::msg("ERR protocol frame exceeds configured limit"));
                        }
                        self.read_buf
                            .extend_from_slice(&temp_bytes[complete_len..n]);
                    }
                    return Ok(temp_bytes[..complete_len].to_vec());
                }
            }
            if self.read_buf.len() + n > MAX_FRAME_BYTES {
                return Err(Error::msg("ERR protocol frame exceeds configured limit"));
            }
            self.read_buf.extend_from_slice(&temp_bytes[..n]);
        }
    }

    pub async fn write_bytes(&mut self, bytes: Vec<u8>) {
        match self.writer.as_mut().expect("connection writer missing") {
            Writer::Direct(writer) => {
                if let Err(e) = writer.write_all(&bytes).await {
                    log::error!("Failed to write to socket; err = {:?}", e);
                }
            }
            Writer::Shared(writer) => writer.write_bytes(bytes).await,
        }
    }

    pub fn shared_writer(&mut self) -> SharedWriter {
        match self.writer.take().expect("connection writer missing") {
            Writer::Direct(writer) => {
                let shared = SharedWriter::new(writer);
                self.writer = Some(Writer::Shared(shared.clone()));
                shared
            }
            Writer::Shared(shared) => {
                self.writer = Some(Writer::Shared(shared.clone()));
                shared
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn connection_pair() -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, server) = tokio::join!(connect, accept);

        (Connection::new(server.unwrap().0), client.unwrap())
    }

    #[tokio::test]
    async fn read_bytes_returns_complete_frame_and_preserves_partial_tail() {
        let (mut connection, mut client) = connection_pair().await;
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nP")
            .await
            .unwrap();

        assert_eq!(
            connection.read_bytes().await.unwrap(),
            b"*1\r\n$4\r\nPING\r\n"
        );

        client.write_all(b"ONG\r\n").await.unwrap();
        assert_eq!(
            connection.read_bytes().await.unwrap(),
            b"*1\r\n$4\r\nPONG\r\n"
        );
    }

    #[tokio::test]
    async fn read_bytes_returns_partial_buffer_or_error_when_peer_closes() {
        let (mut connection, mut client) = connection_pair().await;
        client.write_all(b"*1\r\n$4\r\nPING").await.unwrap();
        client.shutdown().await.unwrap();

        assert_eq!(connection.read_bytes().await.unwrap(), b"*1\r\n$4\r\nPING");

        let (mut empty_connection, mut empty_client) = connection_pair().await;
        empty_client.shutdown().await.unwrap();
        let err = empty_connection.read_bytes().await.unwrap_err();
        assert!(err.to_string().contains("Connection closed by peer"));
    }

    #[tokio::test]
    async fn write_bytes_supports_direct_writer_shared_writer_and_shared_connection_path() {
        let (mut connection, mut client) = connection_pair().await;
        let mut buf = [0_u8; 64];

        connection.write_bytes(b"+direct\r\n".to_vec()).await;
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+direct\r\n");

        let shared = connection.shared_writer();
        shared.write_bytes(b"+shared\r\n".to_vec()).await;
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+shared\r\n");

        let shared_again = connection.shared_writer();
        shared_again.write_bytes(b"+again\r\n".to_vec()).await;
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+again\r\n");

        connection
            .write_bytes(b"+via-connection\r\n".to_vec())
            .await;
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+via-connection\r\n");
    }
}
