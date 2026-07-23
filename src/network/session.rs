use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::{frame::Frame, store::db::Db};

static SESSION_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchedKey {
    pub db_index: usize,
    pub key: String,
    pub key_version: u64,
    pub db_version: u64,
}

#[derive(Clone)]
pub struct Session {
    id: usize,
    certification: bool,
    db: Arc<Db>,
    current_db: usize,
    in_transaction: bool,
    transaction_dirty: bool,
    transaction_frames: Vec<Frame>,
    transaction_bytes: usize,
    watched_keys: Vec<WatchedKey>,
    name: Option<String>,
    created_at: Instant,
    last_interaction_at: Instant,
    last_cmd: Option<String>,
    user: String,
    peer_addr: String,
    local_addr: String,
}

impl Session {
    pub fn new(certification: bool, db: Arc<Db>) -> Self {
        Self::new_with_addresses(
            certification,
            db,
            "127.0.0.1:0".to_string(),
            "127.0.0.1:0".to_string(),
        )
    }

    pub fn new_with_addresses(
        certification: bool,
        db: Arc<Db>,
        peer_addr: String,
        local_addr: String,
    ) -> Self {
        let id = SESSION_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();
        Session {
            id,
            certification,
            db,
            current_db: 0,
            in_transaction: false,
            transaction_dirty: false,
            transaction_frames: Vec::new(),
            transaction_bytes: 0,
            watched_keys: Vec::new(),
            name: None,
            created_at: now,
            last_interaction_at: now,
            last_cmd: None,
            user: "default".to_string(),
            peer_addr,
            local_addr,
        }
    }

    pub fn set_current_db(&mut self, current_db: usize) {
        self.current_db = current_db;
    }

    pub fn get_current_db(&self) -> usize {
        self.current_db
    }

    pub fn set_db(&mut self, db: Arc<Db>) {
        self.db = db;
    }

    pub fn get_db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn set_certification(&mut self, certification: bool) {
        self.certification = certification;
    }

    pub fn get_certification(&self) -> bool {
        self.certification
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    // 事务相关方法
    pub fn start_transaction(&mut self) {
        self.in_transaction = true;
        self.transaction_dirty = false;
        self.transaction_frames.clear();
        self.transaction_bytes = 0;
    }

    pub fn is_in_transaction(&self) -> bool {
        self.in_transaction
    }

    pub fn add_transaction_frame(&mut self, frame: Frame) {
        self.transaction_bytes = self
            .transaction_bytes
            .saturating_add(frame.as_bytes().len());
        self.transaction_frames.push(frame);
    }

    pub fn try_add_transaction_frame(
        &mut self,
        frame: Frame,
        max_commands: usize,
        max_bytes: usize,
    ) -> bool {
        let frame_bytes = frame.as_bytes().len();
        if self.transaction_frames.len() >= max_commands
            || self.transaction_bytes.saturating_add(frame_bytes) > max_bytes
        {
            return false;
        }
        self.transaction_bytes += frame_bytes;
        self.transaction_frames.push(frame);
        true
    }

    pub fn mark_transaction_dirty(&mut self) {
        self.transaction_dirty = true;
    }

    pub fn is_transaction_dirty(&self) -> bool {
        self.transaction_dirty
    }

    pub fn get_transaction_frames(&self) -> &Vec<Frame> {
        &self.transaction_frames
    }

    pub fn transaction_command_count(&self) -> usize {
        self.transaction_frames.len()
    }

    pub fn transaction_bytes(&self) -> usize {
        self.transaction_bytes
    }

    pub fn clear_transaction(&mut self) {
        self.in_transaction = false;
        self.transaction_dirty = false;
        self.transaction_frames.clear();
        self.transaction_bytes = 0;
    }

    pub fn watch_key(&mut self, watched: WatchedKey) {
        if let Some(existing) = self
            .watched_keys
            .iter_mut()
            .find(|entry| entry.db_index == watched.db_index && entry.key == watched.key)
        {
            *existing = watched;
            return;
        }
        self.watched_keys.push(watched);
    }

    pub fn watched_keys(&self) -> &[WatchedKey] {
        &self.watched_keys
    }

    pub fn clear_watches(&mut self) {
        self.watched_keys.clear();
    }

    pub fn set_name(&mut self, name: Option<String>) {
        self.name = name;
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn set_last_cmd(&mut self, command: String) {
        self.last_cmd = Some(command);
        self.last_interaction_at = Instant::now();
    }

    pub fn last_cmd(&self) -> Option<&str> {
        self.last_cmd.as_deref()
    }

    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    pub fn idle_secs(&self) -> u64 {
        self.last_interaction_at.elapsed().as_secs()
    }

    pub fn connected_at(&self) -> Instant {
        self.created_at
    }

    pub fn last_interaction_at(&self) -> Instant {
        self.last_interaction_at
    }

    pub fn set_user(&mut self, user: String) {
        self.user = user;
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn peer_addr(&self) -> &str {
        &self.peer_addr
    }

    pub fn local_addr(&self) -> &str {
        &self.local_addr
    }
}
