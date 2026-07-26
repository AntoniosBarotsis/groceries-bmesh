use std::sync::Arc;

use groceries_bmesh_core::crdt::PeerState;

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
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

    let state_clone = state.clone();
    let _heartbeat = groceries_bmesh_core::start_heartbeat_loop(state_clone);
    let state_clone = state.clone();
    let _respond = groceries_bmesh_core::start_respond_loop(receiver, state_clone);

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(state)
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
