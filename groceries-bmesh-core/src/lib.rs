#![allow(clippy::missing_panics_doc)]

pub mod crdt;

use std::{
  io::{Cursor, Read},
  sync::Arc,
  time::Duration,
};

use flate2::read::GzDecoder;
use iroh::{
  Endpoint, SecretKey,
  endpoint::{IdleTimeout, QuicTransportConfig, presets},
  protocol::Router,
};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
// use iroh_ble_transport::{BleTransport, Central, CentralConfig, Peripheral};
pub use iroh_gossip::api::GossipReceiver;
use iroh_gossip::{
  Gossip,
  api::Event,
  // proto::HyparviewConfig,
};
pub use iroh_topic_tracker::{TopicDiscoveryConfig, TopicDiscoveryExt, TopicDiscoveryHandle};
pub use n0_future::StreamExt;
use sha2::{Digest, Sha256};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{debug, info};

use crate::crdt::{NetMessage, PeerState};

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

pub fn start_respond_loop(
  mut receiver: GossipReceiver,
  state: Arc<RwLock<PeerState>>,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    while let Some(event) = receiver.next().await {
      debug!("RECEIVED {:?}", &event);
      if let Ok(Event::NeighborUp(_key)) = event {
        state.write().await.peers_connected += 1;
        info!("New neighbor");
      }
      if let Ok(Event::NeighborDown(_key)) = event {
        state.write().await.peers_connected -= 1;
        info!("Neighbor down");
      }
      if let Ok(Event::Received(msg)) = event {
        let mut d = GzDecoder::new(Cursor::new(msg.content));
        let mut buf = vec![];
        d.read_to_end(&mut buf).unwrap();

        if let Ok(msg) = postcard::from_bytes::<NetMessage>(&buf).map(|msg| msg.body) {
          debug!("PARSED {:?}", &msg);
          state.write().await.handle_message(msg).await;
        } else {
          debug!("Could not parse {:?}", &buf);
        }
      }
    }
  })
}

pub async fn setup(
  id: u8,
) -> anyhow::Result<(GossipReceiver, Router, TopicDiscoveryHandle, PeerState)> {
  info!(id = id);
  let secret_key = get_key(id)?;

  let config = QuicTransportConfig::builder()
    .keep_alive_interval(Duration::from_secs(5))
    .max_idle_timeout(IdleTimeout::try_from(Duration::from_secs(15)).ok())
    .build();

  // let central = Arc::new(
  //   Central::with_config(CentralConfig {
  //     connect_timeout: Some(std::time::Duration::from_secs(10)),
  //     ..Default::default()
  //   })
  //   .await
  //   .expect("Could not init central config"),
  // );
  // let peripheral = Arc::new(Peripheral::new().await.expect("could not init peripheral"));

  // let ble = iroh_ble_transport::BleTransport::builder()
  //   .central(central)
  //   .peripheral(peripheral)
  //   .build(secret_key.public())
  //   .await            .map_err(|e| {
  //       let msg = e.to_string();
  //       if msg.contains("adapter not found") || msg.contains("AdapterNotFound") {
  //         anyhow!("Bluetooth is not available on this device. A physical Bluetooth adapter is required — simulators and emulators are not supported.")
  //       } else if msg.contains("not powered")
  //           || msg.contains("timed out waiting for Bluetooth")
  //           || msg.contains("power on")
  //       {
  //         anyhow!("Bluetooth is turned off. Please enable Bluetooth in Settings and restart the app.")
  //       } else {
  //           anyhow!(msg)
  //       }
  //   })?;

  let endpoint = Endpoint::builder(presets::N0DisableRelay)
    .secret_key(secret_key.clone())
    .address_lookup(iroh_mdns_address_lookup::MdnsAddressLookup::builder())
    // .hooks(ble.dedup_hook())
    // .add_custom_transport(ble.as_custom_transport())
    // .address_lookup(ble.address_lookup())
    // .clear_ip_transports()
    .transport_config(config)
    .bind()
    .await?;

  // let hyparview = HyparviewConfig {
  //   active_view_capacity: 3,
  //   passive_view_capacity: 12,
  //   shuffle_interval: std::time::Duration::from_secs(120),
  //   ..Default::default()
  // };

  let store = MemStore::new();
  let blobs = BlobsProtocol::new(&store, None);

  let gossip = Gossip::builder()
    // .membership_config(hyparview)
    .spawn(endpoint.clone());

  let router = Router::builder(endpoint.clone())
    .accept(iroh_gossip::ALPN, gossip.clone())
    .accept(iroh_blobs::ALPN, blobs.clone())
    .spawn();

  // TODO: Rename this
  let topic_name = "com.example.myapp.mytopic";
  let mut hasher = Sha256::new();
  hasher.update(topic_name.as_bytes());
  let topic_id = hasher.finalize().to_vec();

  let config = TopicDiscoveryConfig::builder(endpoint.clone())
    .max_peers_per_round(Some(5))
    // .discovery_interval(Duration::from_secs(60))
    // .discovery_interval_no_peers(Duration::from_secs(2))
    .connection_timeout(Duration::from_secs(30))
    .build();

  let (sender, receiver, discovery_handle) = gossip
    .subscribe_with_discovery(topic_id, vec![], config)
    .await?;

  let peerstate = PeerState::new(secret_key.public(), sender, blobs, endpoint);

  Ok((receiver, router, discovery_handle, peerstate))
}
