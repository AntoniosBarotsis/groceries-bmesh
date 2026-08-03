use std::{
  collections::{BTreeMap, HashMap},
  io::{Cursor, Read, Write},
  path::PathBuf,
};

use crdts::{CmRDT, CvRDT, MVReg, Map, VClock, map};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use iroh::{Endpoint, PublicKey};
use iroh_blobs::{BlobsProtocol, api::downloader::Shuffled, ticket::BlobTicket};
use iroh_gossip::api::GossipSender;
use serde::{Deserialize, Serialize};
use tracing::debug;

pub type Actor = PublicKey;
pub type Grocery = MVReg<bool, Actor>;
pub type Groceries = Map<String, Grocery, Actor>;
pub type Clock = VClock<Actor>;
// const MAX_MESSAGE_SIZE: usize = iroh_gossip::proto::DEFAULT_MAX_MESSAGE_SIZE;
const MAX_MESSAGE_SIZE: usize = 200;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OpLog {
  up_by_actor: HashMap<Actor, BTreeMap<u64, map::Op<String, Grocery, Actor>>>,
  removes: Vec<map::Op<String, Grocery, Actor>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SaveData {
  pub map: Groceries,
  pub log: OpLog,
  pub rm_clock: Clock,
}

#[derive(Debug)]
pub struct PeerState {
  pub map: Groceries,
  pub log: OpLog,
  pub sender: GossipSender,
  pub actor: Actor,
  pub blobs: BlobsProtocol,
  pub endpoint: Endpoint,
  pub rm_clock: Clock,
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
  pub fn new(actor: Actor, sender: GossipSender, blobs: BlobsProtocol, endpoint: Endpoint) -> Self {
    Self {
      map: Groceries::new(),
      log: OpLog {
        up_by_actor: HashMap::new(),
        removes: Vec::new(),
      },
      sender,
      actor,
      blobs,
      endpoint,
      rm_clock: VClock::new(),
    }
  }

  fn serialize_compress_msg(msg: CoreMessage) -> Vec<u8> {
    let msg = NetMessage {
      body: msg,
      nonce: rand::random(),
    }
    .to_vec();

    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(&msg).expect("Could not write bytes");
    let compressed_bytes = e.finish().expect("Could not compress");

    debug!(
      "compressed_bytes from {} to {}",
      msg.len(),
      compressed_bytes.len()
    );

    compressed_bytes
  }

  /// Tries to broadcast and falls back to a blob if it can't
  async fn try_broadcast(&self, msg: CoreMessage) {
    // Try to broadcast it, if we can't then we need to send a blob
    if let Err(serialized) = self.broadcast(msg).await {
      let ticket = self.store_blob(serialized).await;
      let msg = CoreMessage::Blob { ticket };
      debug!("Sending blob");
      let _res = self.broadcast(msg).await;
    }
  }

  // FIXME: Apparently this can fail after I ctrl-c because the receiver gets deallocated. Not sure I care though.
  async fn broadcast(&self, msg: CoreMessage) -> Result<(), Vec<u8>> {
    let compressed_bytes = Self::serialize_compress_msg(msg);

    if compressed_bytes.len() > MAX_MESSAGE_SIZE {
      debug!(
        "Message length ({} bytes) exceeded {} bytes",
        compressed_bytes.len(),
        MAX_MESSAGE_SIZE
      );

      return Err(compressed_bytes);
    }

    self
      .sender
      .broadcast(compressed_bytes.into())
      .await
      .expect("Broadcast failed");

    Ok(())
  }

  async fn store_blob(&self, data: Vec<u8>) -> BlobTicket {
    let tag = self
      .blobs
      .add_bytes(data)
      .await
      .expect("Could not add blob to storage");

    BlobTicket::new(self.endpoint.id().into(), tag.hash, tag.format)
  }

  async fn load_blob(&self, ticket: BlobTicket) -> CoreMessage {
    debug!("Blob received with hash = {}", ticket.hash());

    let _res = self
      .blobs
      .downloader(&self.endpoint)
      .download(ticket.hash(), Shuffled::new(vec![ticket.addr().id]))
      .await;

    let blob = self
      .blobs
      .get_bytes(ticket.hash())
      .await
      .expect("Could not download blob");

    let mut d = GzDecoder::new(Cursor::new(blob));
    let mut buf = vec![];
    d.read_to_end(&mut buf).unwrap();

    postcard::from_bytes::<CoreMessage>(&buf).expect("Could not decode")
  }

  pub async fn send_heartbeat(&self) {
    let add_clock = self.map.read_ctx().add_clock;
    let rm_clock = self.rm_clock.clone();
    let msg = CoreMessage::Heartbeat {
      add_clock,
      rm_clock,
    };

    let _res = self.broadcast(msg).await;
  }

  pub async fn handle_message(&mut self, msg: CoreMessage) {
    match msg {
      CoreMessage::Op(op) => {
        self.log.record_op(&op);
        self.map.apply(op.clone());

        if let map::Op::Rm { clock, .. } = op {
          self.rm_clock.merge(clock);
        }
      }
      CoreMessage::Heartbeat {
        add_clock: remote_add_clock,
        rm_clock: remote_rm_clock,
      } => {
        let missing_ops = self.log.missing_ops(&remote_add_clock, &remote_rm_clock);

        // TODO: This if block is super messy, I should make a try_send method instead or something
        #[allow(clippy::if_not_else)]
        if !missing_ops.is_empty() {
          let msg = CoreMessage::AntiEntropyResponse { ops: missing_ops };

          // Try to broadcast it, if we can't then we send a snapshot
          if let Err(_serialized) = self.broadcast(msg).await {
            let state = self.map.clone();
            let msg = CoreMessage::SnapshotResponse {
              state,
              rm_clock: self.rm_clock.clone(),
            };
            self.try_broadcast(msg).await;
          }
        } else {
          // if we don't have any, compare clocks
          let add_clock = self.map.read_ctx().add_clock;
          let rm_clock = self.rm_clock.clone();

          if remote_add_clock < add_clock || remote_rm_clock < rm_clock {
            // they are missing the snapshot – send the full state
            let state = self.map.clone();
            let msg = CoreMessage::SnapshotResponse { state, rm_clock };

            self.try_broadcast(msg).await;
          }
        }
      }
      CoreMessage::AntiEntropyResponse { ops } => {
        for op in ops {
          self.log.record_op(&op);
          self.map.apply(op.clone());

          if let map::Op::Rm { clock, .. } = op {
            self.rm_clock.merge(clock);
          }
        }
      }
      CoreMessage::SnapshotResponse {
        state: incoming_state,
        rm_clock: incoming_rm_clock,
      } => {
        self.map.merge(incoming_state);
        self.log.clear();
        self.rm_clock.merge(incoming_rm_clock);
      }
      CoreMessage::Blob { ticket } => {
        let msg = self.load_blob(ticket).await;

        debug!("Decoded blob");
        // SAFETY: Since Blobs can only ever contain SnapshotResponses, recursion will only ever be 1 step deep
        assert!(
          matches!(msg, CoreMessage::SnapshotResponse { .. }),
          "Blob should only ever contain a SnapshotResponse"
        );

        Box::pin(self.handle_message(msg)).await;
        debug!("Handled blob");
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

  pub fn clear(&mut self) {
    self.map = Groceries::new();
    self.log.clear();
    self.rm_clock = VClock::new();
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

    if let map::Op::Rm { clock, .. } = &op {
      self.rm_clock.merge(clock.to_owned());
    }

    let msg = CoreMessage::Op(op);
    let _res = self.broadcast(msg).await;
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
      rm_clock: self.rm_clock.clone(),
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
    self.rm_clock = data.rm_clock;

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
  pub fn missing_ops(
    &self,
    remote_add_clock: &VClock<Actor>,
    remote_rm_clock: &VClock<Actor>,
  ) -> Vec<map::Op<String, Grocery, Actor>> {
    let mut missing = vec![];

    for (actor, ops_by_counter) in &self.up_by_actor {
      let remote_counter = remote_add_clock.get(actor);

      // Send all ops with counter > remote_counter
      for (_counter, op) in ops_by_counter.range(remote_counter + 1..) {
        missing.push(op.clone());
      }
    }

    for op in &self.removes {
      if let map::Op::Rm { clock, .. } = op {
        // Remote needs this remove if its clock does NOT yet dominate the remove's context.
        if !remote_rm_clock.ge(clock) {
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
    add_clock: Clock,
    rm_clock: Clock,
  },
  AntiEntropyResponse {
    ops: Vec<map::Op<String, Grocery, Actor>>,
  },
  SnapshotResponse {
    state: Groceries,
    rm_clock: Clock,
  },
  Blob {
    ticket: BlobTicket,
  },
}
