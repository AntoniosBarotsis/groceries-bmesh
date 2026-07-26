use std::collections::{BTreeMap, HashMap};

use crdts::{CmRDT, CvRDT, MVReg, Map, VClock, map};
use iroh::PublicKey;
use iroh_gossip::api::GossipSender;
use serde::{Deserialize, Serialize};

pub type Actor = PublicKey;
pub type Grocery = MVReg<String, Actor>;
pub type Groceries = Map<String, Grocery, Actor>;
pub type Clock = VClock<Actor>;

#[derive(Debug)]
pub struct OpLog {
  up_by_actor: HashMap<Actor, BTreeMap<u64, map::Op<String, Grocery, Actor>>>,
  removes: Vec<map::Op<String, Grocery, Actor>>,
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
    serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
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

  async fn broadcast(&self, msg: CoreMessage) {
    // FIXME: Apparently this can fail after I ctrl-c because the receiver gets deallocated. Not sure I care though.
    self
      .sender
      .broadcast(
        NetMessage {
          body: msg,
          nonce: rand::random(),
        }
        .to_vec()
        .into(),
      )
      .await
      .expect("Broadcast failed");
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

        let msg = CoreMessage::AntiEntropyResponse { ops: missing_ops };
        self.broadcast(msg).await;
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

  pub async fn insert(&mut self, key: String, value: String) {
    let ctx = self.map.read_ctx().derive_add_ctx(self.actor);
    let op = self.map.update(key, ctx, |v, ctx| v.write(value, ctx));

    self.apply_local_op(op).await;
  }

  pub async fn update(&mut self, key: String, f: impl FnOnce(&Grocery) -> String) {
    let ctx = self.map.read_ctx().derive_add_ctx(self.actor);
    let op = self.map.update(key, ctx, |v, ctx| v.write(f(v), ctx));

    self.apply_local_op(op).await;
  }

  pub async fn remove(&mut self, key: String) {
    // doesnt look like this got applied locally
    let ctx = self.map.read_ctx().derive_rm_ctx();
    let op = self.map.rm(key, ctx);

    self.apply_local_op(op).await;
  }

  /// Add mutation to oplog and broadcast it
  async fn apply_local_op(&mut self, op: map::Op<String, Grocery, Actor>) {
    self.log.record_op(&op);
    self.map.apply(op.clone());

    let msg = CoreMessage::Op(op);
    self.broadcast(msg).await;
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
