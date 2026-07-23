use anyhow::Error;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::frame::{Frame, FrameScanResult, MAX_FRAME_BYTES};

const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const INCOMPLETE_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const SHARED_WRITE_QUEUE_CAPACITY: usize = 64;
const SHARED_WRITE_QUEUE_BYTES: usize = 16 * 1024 * 1024;
const SHARED_WRITE_MAX_MESSAGE_BYTES: usize = MAX_FRAME_BYTES + 1024 * 1024;
const SOCKET_READ_BUFFER_BYTES: usize = 16 * 1024;

struct QueuedWrite {
    chunks: Vec<Arc<[u8]>>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct SharedWriter {
    sender: mpsc::Sender<QueuedWrite>,
    budget: Arc<Semaphore>,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
}

impl SharedWriter {
    fn new(writer: OwnedWriteHalf) -> Self {
        let (sender, mut receiver) = mpsc::channel::<QueuedWrite>(SHARED_WRITE_QUEUE_CAPACITY);
        let budget = Arc::new(Semaphore::new(SHARED_WRITE_QUEUE_BYTES));
        let worker_budget = budget.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let worker_closed = closed.clone();
        let close_notify = Arc::new(Notify::new());
        let worker_close_notify = close_notify.clone();
        tokio::spawn(async move {
            let mut writer = writer;
            loop {
                let close_requested = worker_close_notify.notified();
                tokio::pin!(close_requested);
                close_requested.as_mut().enable();
                if worker_closed.load(Ordering::Acquire) {
                    break;
                }
                let queued = tokio::select! {
                    biased;
                    _ = &mut close_requested => break,
                    queued = receiver.recv() => {
                        let Some(queued) = queued else {
                            break;
                        };
                        queued
                    }
                };
                let write = async {
                    for chunk in &queued.chunks {
                        writer.write_all(chunk).await?;
                    }
                    std::io::Result::Ok(())
                };
                match tokio::time::timeout(SOCKET_WRITE_TIMEOUT, write).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        log::debug!("Failed to write to socket; err = {err:?}");
                        SharedWriter::mark_closed(&worker_closed, &worker_close_notify);
                        worker_budget.close();
                        break;
                    }
                    Err(_) => {
                        log::warn!("Timed out writing to socket");
                        SharedWriter::mark_closed(&worker_closed, &worker_close_notify);
                        worker_budget.close();
                        break;
                    }
                }
            }
            let _ = writer.shutdown().await;
        });
        Self {
            sender,
            budget,
            closed,
            close_notify,
        }
    }

    pub async fn write_bytes(&self, bytes: Vec<u8>) -> bool {
        self.write_shared(Arc::from(bytes)).await
    }

    async fn write_shared(&self, bytes: Arc<[u8]>) -> bool {
        self.write_chunks(vec![bytes]).await
    }

    async fn write_chunks(&self, chunks: Vec<Arc<[u8]>>) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let Some(total_bytes) = Self::chunks_len(&chunks) else {
            self.close();
            return false;
        };
        let Some(permits) = Self::queue_permits(total_bytes) else {
            self.close();
            return false;
        };
        let permit = match tokio::time::timeout(
            SOCKET_WRITE_TIMEOUT,
            self.budget.clone().acquire_many_owned(permits),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) => {
                self.close();
                return false;
            }
        };
        if self.is_closed() {
            return false;
        }
        let queued = matches!(
            tokio::time::timeout(
                SOCKET_WRITE_TIMEOUT,
                self.sender.send(QueuedWrite {
                    chunks,
                    _permit: permit,
                }),
            )
            .await,
            Ok(Ok(()))
        );
        if !queued {
            self.close();
        }
        queued
    }

    pub fn try_write_bytes(&self, bytes: Vec<u8>) -> bool {
        self.try_write_shared(Arc::from(bytes))
    }

    pub(crate) fn try_write_shared(&self, bytes: Arc<[u8]>) -> bool {
        self.try_write_chunks(vec![bytes])
    }

    pub(crate) fn try_write_chunks(&self, chunks: Vec<Arc<[u8]>>) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let Some(total_bytes) = Self::chunks_len(&chunks) else {
            self.close();
            return false;
        };
        let Some(permits) = Self::queue_permits(total_bytes) else {
            self.close();
            return false;
        };
        let Ok(permit) = self.budget.clone().try_acquire_many_owned(permits) else {
            self.close();
            return false;
        };
        if self.is_closed() {
            return false;
        }
        let queued = self
            .sender
            .try_send(QueuedWrite {
                chunks,
                _permit: permit,
            })
            .is_ok();
        if !queued {
            self.close();
        }
        queued
    }

    fn chunks_len(chunks: &[Arc<[u8]>]) -> Option<usize> {
        chunks
            .iter()
            .try_fold(0usize, |total, chunk| total.checked_add(chunk.len()))
    }

    fn queue_permits(bytes: usize) -> Option<u32> {
        if bytes > SHARED_WRITE_MAX_MESSAGE_BYTES {
            return None;
        }
        u32::try_from(bytes.clamp(1, SHARED_WRITE_QUEUE_BYTES)).ok()
    }

    fn close(&self) {
        Self::mark_closed(&self.closed, &self.close_notify);
        self.budget.close();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn close_for_test(&self) {
        self.close();
    }

    fn mark_closed(closed: &AtomicBool, close_notify: &Notify) {
        if !closed.swap(true, Ordering::AcqRel) {
            close_notify.notify_waiters();
        }
    }

    async fn wait_closed(&self) {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return;
            }

            let notified = self.close_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
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
    partial_frame_started_at: Option<tokio::time::Instant>,
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
            partial_frame_started_at: None,
            writer: Some(Writer::Direct(writer)),
        }
    }

    pub async fn read_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let mut temp_bytes = [0; SOCKET_READ_BUFFER_BYTES];

        loop {
            if matches!(
                self.writer.as_ref(),
                Some(Writer::Shared(writer)) if writer.is_closed()
            ) {
                return Err(Error::msg("Connection writer closed"));
            }
            if self.read_buf.len() > MAX_FRAME_BYTES {
                return Err(Error::msg("ERR protocol frame exceeds configured limit"));
            }
            match Frame::scan_complete_frames(&self.read_buf) {
                FrameScanResult::Ready(complete_len) => {
                    let mut bytes = std::mem::take(&mut self.read_buf);
                    self.read_buf = bytes.split_off(complete_len);
                    if self.read_buf.is_empty() {
                        self.partial_frame_started_at = None;
                    } else {
                        self.partial_frame_started_at = Some(tokio::time::Instant::now());
                    }
                    return Ok(bytes);
                }
                FrameScanResult::Invalid(message) => {
                    return Err(Error::msg(format!("ERR Protocol error: {message}")));
                }
                FrameScanResult::Incomplete => {}
            }

            let shared_writer = match self.writer.as_ref() {
                Some(Writer::Shared(writer)) => Some(writer.clone()),
                _ => None,
            };
            let read = async {
                let socket_read = self.reader.read(&mut temp_bytes);
                if let Some(started_at) = self.partial_frame_started_at {
                    let deadline = started_at + INCOMPLETE_FRAME_TIMEOUT;
                    tokio::time::timeout_at(deadline, socket_read)
                        .await
                        .map_err(|_| {
                            Error::msg("ERR Protocol error: timeout reading incomplete frame")
                        })
                } else {
                    Ok(socket_read.await)
                }
            };
            let read_result = if let Some(writer) = shared_writer {
                tokio::select! {
                    result = read => result?,
                    _ = writer.wait_closed() => {
                        return Err(Error::msg("Connection writer closed"));
                    }
                }
            } else {
                read.await?
            };
            let n = match read_result {
                Ok(n) => n,
                Err(e) => {
                    return Err(Error::msg(format!("Failed to read from stream: {:?}", e)));
                }
            };

            if n == 0 {
                if self.read_buf.is_empty() {
                    return Err(Error::msg("Connection closed by peer"));
                } else {
                    return Err(Error::msg(
                        "ERR Protocol error: unexpected end of incomplete frame",
                    ));
                }
            }
            if self.read_buf.is_empty() {
                match Frame::scan_complete_frames(&temp_bytes[..n]) {
                    FrameScanResult::Ready(complete_len) => {
                        if complete_len < n {
                            if n - complete_len > MAX_FRAME_BYTES {
                                return Err(Error::msg(
                                    "ERR protocol frame exceeds configured limit",
                                ));
                            }
                            self.read_buf
                                .extend_from_slice(&temp_bytes[complete_len..n]);
                            self.partial_frame_started_at = Some(tokio::time::Instant::now());
                        }
                        return Ok(temp_bytes[..complete_len].to_vec());
                    }
                    FrameScanResult::Invalid(message) => {
                        return Err(Error::msg(format!("ERR Protocol error: {message}")));
                    }
                    FrameScanResult::Incomplete => {}
                }
            }
            if self.read_buf.len() + n > MAX_FRAME_BYTES {
                return Err(Error::msg("ERR protocol frame exceeds configured limit"));
            }
            if self.read_buf.is_empty() {
                self.partial_frame_started_at = Some(tokio::time::Instant::now());
            }
            self.read_buf.extend_from_slice(&temp_bytes[..n]);
        }
    }

    pub async fn write_bytes(&mut self, bytes: Vec<u8>) -> bool {
        match self.writer.as_mut().expect("connection writer missing") {
            Writer::Direct(writer) => {
                match tokio::time::timeout(SOCKET_WRITE_TIMEOUT, writer.write_all(&bytes)).await {
                    Ok(Ok(())) => true,
                    Ok(Err(err)) => {
                        log::debug!("Failed to write to socket; err = {err:?}");
                        false
                    }
                    Err(_) => {
                        log::warn!("Timed out writing to socket");
                        false
                    }
                }
            }
            Writer::Shared(writer) => writer.write_bytes(bytes).await,
        }
    }

    pub async fn wait_read_closed(&mut self) -> Result<(), Error> {
        let mut temp_bytes = [0_u8; SOCKET_READ_BUFFER_BYTES];
        loop {
            if matches!(
                self.writer.as_ref(),
                Some(Writer::Shared(writer)) if writer.is_closed()
            ) {
                return Err(Error::msg("Connection writer closed"));
            }
            let shared_writer = match self.writer.as_ref() {
                Some(Writer::Shared(writer)) => Some(writer.clone()),
                _ => None,
            };
            let read = self.reader.read(&mut temp_bytes);
            let read_result = if let Some(writer) = shared_writer {
                tokio::select! {
                    result = read => result,
                    _ = writer.wait_closed() => {
                        return Err(Error::msg("Connection writer closed"));
                    }
                }
            } else {
                read.await
            };
            let n = read_result
                .map_err(|err| Error::msg(format!("Failed to wait for socket close: {err}")))?;
            if n == 0 {
                return Ok(());
            }
            if self.read_buf.len().saturating_add(n) > MAX_FRAME_BYTES {
                return Err(Error::msg("ERR protocol frame exceeds configured limit"));
            }
            if self.read_buf.is_empty() {
                self.partial_frame_started_at = Some(tokio::time::Instant::now());
            }
            self.read_buf.extend_from_slice(&temp_bytes[..n]);
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
    async fn read_bytes_reports_protocol_error_for_partial_frame_at_eof() {
        let (mut connection, mut client) = connection_pair().await;
        client.write_all(b"*1\r\n$4\r\nPING").await.unwrap();
        client.shutdown().await.unwrap();

        let err = connection.read_bytes().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("unexpected end of incomplete frame")
        );

        let (mut empty_connection, mut empty_client) = connection_pair().await;
        empty_client.shutdown().await.unwrap();
        let err = empty_connection.read_bytes().await.unwrap_err();
        assert!(err.to_string().contains("Connection closed by peer"));
    }

    #[tokio::test]
    async fn read_bytes_reports_invalid_frame_without_waiting_for_peer_close() {
        let (mut connection, mut client) = connection_pair().await;
        client.write_all(b"$bad\r\n").await.unwrap();

        let err = tokio::time::timeout(std::time::Duration::from_secs(1), connection.read_bytes())
            .await
            .expect("invalid frame should not wait for more bytes")
            .unwrap_err();
        assert!(err.to_string().contains("ERR Protocol error"));
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

    #[tokio::test]
    async fn shared_writer_rejects_a_message_larger_than_its_memory_budget() {
        let (mut connection, _client) = connection_pair().await;
        let shared = connection.shared_writer();

        assert!(!shared.try_write_bytes(vec![0_u8; SHARED_WRITE_MAX_MESSAGE_BYTES + 1]));
    }

    #[tokio::test]
    async fn wait_read_closed_preserves_input_read_while_a_command_is_blocked() {
        let (mut connection, mut client) = connection_pair().await;
        let mut wait_closed = Box::pin(connection.wait_read_closed());

        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut wait_closed)
                .await
                .is_err()
        );
        drop(wait_closed);

        assert_eq!(
            connection.read_bytes().await.unwrap(),
            b"*1\r\n$4\r\nPING\r\n"
        );
    }

    #[tokio::test]
    async fn wait_read_closed_detects_eof_after_buffering_pipelined_input() {
        let (mut connection, mut client) = connection_pair().await;
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        client.shutdown().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), connection.wait_read_closed())
            .await
            .expect("peer close should be detected even after queued input")
            .unwrap();
    }

    #[tokio::test]
    async fn closing_a_shared_writer_wakes_the_connection_read_loop() {
        let (mut connection, _client) = connection_pair().await;
        let shared = connection.shared_writer();
        assert!(!shared.try_write_bytes(vec![0_u8; SHARED_WRITE_MAX_MESSAGE_BYTES + 1]));

        let err = tokio::time::timeout(Duration::from_secs(1), connection.read_bytes())
            .await
            .expect("closed writer should wake the connection")
            .unwrap_err();
        assert!(err.to_string().contains("Connection writer closed"));
    }
}
