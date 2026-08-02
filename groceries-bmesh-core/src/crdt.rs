use std::{
  collections::{BTreeMap, HashMap},
  io::Write,
  path::PathBuf,
};

use crdts::{CmRDT, CvRDT, MVReg, Map, VClock, map};
use flate2::{Compression, write::GzEncoder};
use iroh::PublicKey;
use iroh_gossip::api::GossipSender;
use serde::{Deserialize, Serialize};

pub type Actor = PublicKey;
pub type Grocery = MVReg<bool, Actor>;
pub type Groceries = Map<String, Grocery, Actor>;
pub type Clock = VClock<Actor>;
// const MAX_MESSAGE_SIZE: usize = iroh_gossip::proto::DEFAULT_MAX_MESSAGE_SIZE;
const MAX_MESSAGE_SIZE: usize = 300;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OpLog {
  up_by_actor: HashMap<Actor, BTreeMap<u64, map::Op<String, Grocery, Actor>>>,
  removes: Vec<map::Op<String, Grocery, Actor>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SaveData {
  pub map: Groceries,
  pub log: OpLog,
}

#[derive(Debug)]
pub struct PeerState {
  pub map: Groceries,
  pub log: OpLog,
  pub sender: GossipSender,
  pub actor: Actor,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetMessage {
  pub body: CoreMessage,
  nonce: [u8; 16],
}

impl NetMessage {
  pub fn to_vec(&self) -> Vec<u8> {
    postcard::to_stdvec(&self).expect("Serialization failed")
  }
}

impl PeerState {
  pub fn new(actor: Actor, sender: GossipSender) -> Self {
    Self {
      map: Groceries::new(),
      log: OpLog {
        up_by_actor: HashMap::new(),
        removes: Vec::new(),
      },
      sender,
      actor,
    }
  }

  async fn serialize_compress_msg(msg: CoreMessage) -> Vec<u8> {
    let msg = NetMessage {
      body: msg,
      nonce: rand::random(),
    }
    .to_vec();

    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(&msg).expect("Could not write bytes");
    let compressed_bytes = e.finish().expect("Could not compress");

    tracing::debug!(
      "compressed_bytes from {} to {}",
      msg.len(),
      compressed_bytes.len()
    );

    compressed_bytes
  }

  async fn broadcast_raw(&self, msg: Vec<u8>) {
    // FIXME: Apparently this can fail after I ctrl-c because the receiver gets deallocated. Not sure I care though.
    self
      .sender
      .broadcast(msg.into())
      .await
      .expect("Broadcast failed");
  }

  // TODO: Maybe it would make sense for this to return a Result
  async fn broadcast(&self, msg: CoreMessage) {
    let compressed_bytes = Self::serialize_compress_msg(msg).await;

    if compressed_bytes.len() > MAX_MESSAGE_SIZE {
      tracing::warn!(
        "Message length ({} bytes) exceeded {} bytes",
        compressed_bytes.len(),
        MAX_MESSAGE_SIZE
      );
    }

    self.broadcast_raw(compressed_bytes).await;
  }

  pub async fn send_heartbeat(&self) {
    let clock = self.map.read_ctx().add_clock;
    let msg = CoreMessage::Heartbeat { clock };
    self.broadcast(msg).await;
  }

  pub async fn handle_message(&mut self, msg: CoreMessage) {
    match msg {
      CoreMessage::Op(op) => {
        self.log.record_op(&op);
        self.map.apply(op);
      }
      CoreMessage::Heartbeat {
        clock: remote_clock,
      } => {
        let missing_ops = self.log.missing_ops(&remote_clock);

        // TODO: This if block is super messy, I should make a try_send method instead or something
        if !missing_ops.is_empty() {
          let msg = CoreMessage::AntiEntropyResponse { ops: missing_ops };
          let serialized = Self::serialize_compress_msg(msg).await;

          // If we can broadcast it, then do that, else send a snapshot
          if serialized.len() < MAX_MESSAGE_SIZE {
            tracing::debug!("Broadcasting AntiEntropyResponse");

            self.broadcast_raw(serialized).await;
          } else {
            let state = self.map.clone();
            let clock = self.map.read_ctx().add_clock;
            let msg = CoreMessage::SnapshotResponse { state, clock };
            let serialized = Self::serialize_compress_msg(msg).await;

            if serialized.len() < MAX_MESSAGE_SIZE {
              tracing::debug!("AntiEntropyResponse too big, broadcasting SnapshotResponse");

              self.broadcast_raw(serialized).await;
            } else {
              // FIXME: The SnapshotResponse can still be over the limit (though that's harder to occur since
              // the state alone would have to be over 4kb compressed). But that can still happen, we need to
              // check and fall back to an iroh-blob here.
              tracing::error!("SnapshotResponse is too big to send, need to implement blobs");
            }
          }
        } else {
          // if we don't have any, compare clocks
          let our_clock = self.map.read_ctx().add_clock;
          if remote_clock < our_clock {
            // they are missing the snapshot – send the full state
            let state = self.map.clone();
            let clock = self.map.read_ctx().add_clock;
            let msg = CoreMessage::SnapshotResponse { state, clock };
            // FIXME: This can also fail
            self.broadcast(msg).await;
          }
        }
      }
      CoreMessage::AntiEntropyResponse { ops } => {
        for op in ops {
          self.log.record_op(&op);
          self.map.apply(op);
        }
      }
      // TODO: I never send this. I could probably remove it, the only concern is that its faster than AntiEntropyResponse.
      // Maybe I can check if the heartbeat is really far back and in that case not respond with AntiEntropyResponse and
      // have the other node notice that and send a SnapshotRequest?
      CoreMessage::SnapshotRequest => {
        let state = self.map.clone();
        let clock = self.map.read_ctx().add_clock;
        let msg = CoreMessage::SnapshotResponse { state, clock };
        self.broadcast(msg).await;
      }
      CoreMessage::SnapshotResponse {
        state: incoming_state,
        clock: _,
      } => {
        self.map.merge(incoming_state);
        self.log.clear();
      }
    }
  }

  pub fn get(&self, key: &str) -> Option<Grocery> {
    self.map.get(&key.to_owned()).val
  }

  pub async fn update(&mut self, key: String, value: bool) {
    let ctx = self.map.read_ctx().derive_add_ctx(self.actor);
    let op = self.map.update(key, ctx, |v, ctx| v.write(value, ctx));

    self.apply_local_op(op).await;
  }

  pub async fn remove(&mut self, key: String) {
    let mut ctx = self.map.read_ctx();

    // increment rm clock manually
    // https://github.com/rust-crdt/rust-crdt/issues/160
    let new_dot = ctx.rm_clock.inc(self.actor);
    ctx.rm_clock.apply(new_dot);

    let rm_ctx = crdts::ctx::RmCtx {
      clock: ctx.rm_clock.clone(),
    };
    let op = self.map.rm(key, rm_ctx);

    self.apply_local_op(op).await;
  }

  /// Add mutation to oplog and broadcast it
  async fn apply_local_op(&mut self, op: map::Op<String, Grocery, Actor>) {
    self.log.record_op(&op);
    self.map.apply(op.clone());

    let msg = CoreMessage::Op(op);
    self.broadcast(msg).await;
  }

  pub fn to_hashmap(&self) -> HashMap<String, String> {
    let mut hash_map: HashMap<String, String> = HashMap::new();
    for item_ctx in self.map.iter() {
      let (key, value) = item_ctx.val;
      let value_ctx = value.read();

      // TODO: Make sure this is fine
      let value_string = value_ctx.val.iter().any(|el| *el).to_string();

      let _unused = hash_map.insert(key.to_owned(), value_string);
    }
    hash_map
  }

  pub async fn write_to_file(&self, path: PathBuf) -> Result<(), String> {
    let data = SaveData {
      map: self.map.clone(),
      log: self.log.clone(),
    };

    let json = serde_json::to_string(&data).map_err(|e| e.to_string())?;

    tokio::fs::write(&path, json)
      .await
      .map_err(|e| e.to_string())?;

    Ok(())
  }

  pub async fn load_from_file(&mut self, path: PathBuf) -> Result<(), String> {
    let json = tokio::fs::read_to_string(path)
      .await
      .map_err(|e| e.to_string())?;

    let data = serde_json::from_str::<SaveData>(&json).map_err(|e| e.to_string())?;

    self.map = data.map;
    self.log = data.log;

    Ok(())
  }
}

impl OpLog {
  fn clear(&mut self) {
    self.up_by_actor.clear();
    self.removes.clear();
  }

  fn record_op(&mut self, map_op: &map::Op<String, Grocery, Actor>) {
    match map_op {
      map::Op::Rm { .. } => {
        self.removes.push(map_op.clone());
      }
      map::Op::Up { dot, key: _, op: _ } => {
        let _ = self
          .up_by_actor
          .entry(dot.actor)
          .or_default()
          .entry(dot.counter)
          .or_insert_with(|| map_op.clone());
      }
    }
  }

  /// Returns all ops that the remote peer is missing
  pub fn missing_ops(&self, remote_clock: &VClock<Actor>) -> Vec<map::Op<String, Grocery, Actor>> {
    let mut missing = vec![];

    for (actor, ops_by_counter) in &self.up_by_actor {
      let remote_counter = remote_clock.get(actor);

      // Send all ops with counter > remote_counter
      for (_counter, op) in ops_by_counter.range(remote_counter + 1..) {
        missing.push(op.clone());
      }
    }

    for op in &self.removes {
      if let map::Op::Rm { clock, .. } = op {
        // Remote needs this remove if its clock does NOT yet dominate the remove's context.
        if !remote_clock.ge(clock) {
          missing.push(op.clone());
        }
      }
    }

    missing
  }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CoreMessage {
  Op(map::Op<String, Grocery, Actor>),
  Heartbeat {
    clock: Clock,
  },
  AntiEntropyResponse {
    ops: Vec<map::Op<String, Grocery, Actor>>,
  },
  SnapshotRequest,
  SnapshotResponse {
    state: Groceries,
    clock: Clock,
  },
}
