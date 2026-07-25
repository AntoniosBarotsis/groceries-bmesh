#![allow(clippy::missing_panics_doc)]

pub mod crdt;

use std::{sync::Arc, time::Duration};

use iroh::{
  Endpoint, SecretKey,
  endpoint::{IdleTimeout, QuicTransportConfig, presets},
  protocol::Router,
};
use iroh_gossip::{
  Gossip,
  api::{GossipReceiver, GossipSender},
};
use iroh_topic_tracker::{TopicDiscoveryConfig, TopicDiscoveryExt, TopicDiscoveryHandle};
use sha2::{Digest, Sha256};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::info;

use crate::crdt::{Actor, PeerState};

// pub fn get_key() -> anyhow::Result<SecretKey> {
//   let key_path = PathBuf::from("my_peer_key.bin");
//   let secret_key = if key_path.exists() {
//     let bytes = std::fs::read(&key_path)?;
//     SecretKey::from_bytes(bytes.as_array::<32>().expect("Invalid key length"))
//   } else {
//     let key = SecretKey::generate();
//     std::fs::write(&key_path, key.to_bytes())?;
//     key
//   };

//   Ok(secret_key)
// }

pub fn get_key(id: u8) -> anyhow::Result<SecretKey> {
  Ok(SecretKey::from_bytes(&[id; 32]))
}

pub fn start_heartbeat_loop(state: Arc<RwLock<PeerState>>) -> JoinHandle<()> {
  tokio::spawn(async move {
    loop {
      // debug!("SENDING HEARTBEAT");
      state.read().await.send_heartbeat().await;
      tokio::time::sleep(Duration::from_secs(5)).await;
    }
  })
}

pub async fn setup(
  id: u8,
) -> anyhow::Result<(
  Actor,
  GossipSender,
  GossipReceiver,
  Router,
  TopicDiscoveryHandle,
)> {
  info!(id = id);
  let secret_key = get_key(id)?;

  let config = QuicTransportConfig::builder()
    .keep_alive_interval(Duration::from_secs(5))
    .max_idle_timeout(IdleTimeout::try_from(Duration::from_mins(1)).ok())
    .build();
  let endpoint = Endpoint::builder(presets::N0DisableRelay)
    .secret_key(secret_key.clone())
    .address_lookup(iroh_mdns_address_lookup::MdnsAddressLookup::builder())
    .transport_config(config)
    .bind()
    .await?;

  let gossip = Gossip::builder().spawn(endpoint.clone());

  let router = Router::builder(endpoint.clone())
    .accept(iroh_gossip::ALPN, gossip.clone())
    .spawn();

  // TODO: Rename this
  let topic_name = "com.example.myapp.mytopic";
  let mut hasher = Sha256::new();
  hasher.update(topic_name.as_bytes());
  let topic_id = hasher.finalize().to_vec();

  let config = TopicDiscoveryConfig::builder(endpoint)
    .max_peers_per_round(Some(5))
    // .discovery_interval(Duration::from_secs(60))
    // .discovery_interval_no_peers(Duration::from_secs(2))
    .connection_timeout(Duration::from_secs(30))
    .build();

  let (sender, receiver, discovery_handle) = gossip
    .subscribe_with_discovery(topic_id, vec![], config)
    .await?;

  Ok((
    secret_key.public(),
    sender,
    receiver,
    router,
    discovery_handle,
  ))
}
