use std::{sync::Arc, time::Duration};

use anyhow::Context;
use groceries_bmesh_core::{setup, start_heartbeat_loop, start_respond_loop};
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
  let (receiver, router, _discovery_handle, state) = setup(id).await?;
  info!("Joined");

  let state = Arc::new(RwLock::new(state));

  let _heartbeat = start_heartbeat_loop(state.clone());

  let state_clone = state.clone();
  tokio::spawn(async move {
    loop {
      tokio::time::sleep(Duration::from_secs(10)).await;
      let map = state_clone.read().await.to_hashmap();
      info!("PRINTING VALUES {:?}", map);
    }
  });

  let state_clone = state.clone();
  let _respond = start_respond_loop(receiver, state_clone);

  tokio::signal::ctrl_c().await?;
  warn!("AFTER");

  router.shutdown().await.context("shutdown router")?;

  Ok(())
}
