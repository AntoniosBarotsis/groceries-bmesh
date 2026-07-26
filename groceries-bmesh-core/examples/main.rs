use std::{sync::Arc, time::Duration};

use anyhow::Context;
use groceries_bmesh_core::{crdt::PeerState, setup, start_heartbeat_loop, start_respond_loop};
pub use iroh_gossip::api::Event;
pub use n0_future::StreamExt;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();

  // TODO: This is for testing, change it later
  let id = std::env::var("ID").expect("ID not passed");
  let id = id.parse::<u8>().expect("Could not parse id");
  let (actor, sender, receiver, router, _discovery_handle) = setup(id).await?;
  info!("Joined");

  let state = Arc::new(RwLock::new(PeerState::new(actor, sender.clone())));

  let _heartbeat = start_heartbeat_loop(state.clone());

  // Simulated action sequence
  // let state_clone = state.clone();
  // tokio::spawn(async move {
  //   tokio::time::sleep(Duration::from_secs(7)).await;
  //   info!("Beginning event sequence");

  //   info!("Inserting hello->world");
  //   state_clone
  //     .write()
  //     .await
  //     .insert("hello".to_owned(), "world".to_owned())
  //     .await;

  //   tokio::time::sleep(Duration::from_secs(5)).await;

  //   info!("Updating hello->world 2");
  //   state_clone
  //     .write()
  //     .await
  //     .update("hello".to_owned(), |_v| "world 2".to_owned())
  //     .await;

  //   // tokio::time::sleep(Duration::from_secs(15)).await;
  //   // info!("Removing hello");
  //   // state_clone.write().await.remove("hello".to_owned()).await;
  // });

  // let state_clone = state.clone();
  // tokio::spawn(async move {
  //   loop {
  //     tokio::time::sleep(Duration::from_secs(10)).await;
  //     let map = &state_clone.read().await.map;
  //     let values = map.get(&"hello".to_string()).val.map(|el| el.read().val);
  //     info!("PRINTING VALUES {:?}", values);
  //   }
  // });

  let state_clone = state.clone();
  let _respond = start_respond_loop(receiver, state_clone);

  tokio::signal::ctrl_c().await?;
  warn!("AFTER");

  router.shutdown().await.context("shutdown router")?;

  Ok(())
}
