use groceries_bmesh_core::crdt::PeerState;
use std::collections::HashMap;
use std::sync::Arc;
use tauri::State;
use tokio::sync::RwLock;

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
async fn insert(
    state: State<'_, Arc<RwLock<PeerState>>>,
    key: String,
    value: String,
) -> Result<(), ()> {
    let mut guard = state.write().await;
    guard.insert(key, value).await;

    Ok(())
}

#[tauri::command]
async fn get(state: State<'_, Arc<RwLock<PeerState>>>, key: String) -> Result<(), ()> {
    let mut guard = state.read().await;
    guard.get(&key);

    Ok(())
}

#[tauri::command]
async fn remove(state: State<'_, Arc<RwLock<PeerState>>>, key: String) -> Result<(), ()> {
    let mut guard = state.write().await;
    guard.remove(key).await;

    Ok(())
}

#[tauri::command]
async fn to_hashmap(
    state: State<'_, Arc<RwLock<PeerState>>>,
) -> Result<HashMap<String, String>, ()> {
    let mut guard = state.read().await;
    let res = guard.to_hashmap();

    Ok(res)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub async fn run() {
    // std::env::set_var("RUST_BACKTRACE", "full");

    // TODO: Change this later
    let id = 1;
    let (actor, sender, receiver, _router, _discovery_handle) =
        groceries_bmesh_core::setup(id).await.unwrap();

    let state = Arc::new(tauri::async_runtime::RwLock::new(PeerState::new(
        actor,
        sender.clone(),
    )));

    let _heartbeat = groceries_bmesh_core::start_heartbeat_loop(state.clone());
    let _respond = groceries_bmesh_core::start_respond_loop(receiver, state.clone());

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            greet, insert, get, remove, to_hashmap
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
