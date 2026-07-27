use groceries_bmesh_core::crdt::PeerState;
use std::collections::HashMap;
use std::sync::Arc;
use tauri::Manager;
use tauri::State;
use tauri_plugin_blew::{are_ble_permissions_granted, request_ble_permissions};
use tokio::sync::RwLock;

type AppState = Arc<tokio::sync::RwLock<PeerState>>;

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
async fn update(state: State<'_, AppState>, key: String, value: bool) -> Result<(), ()> {
    let mut guard = state.write().await;
    guard.update(key, value).await;

    Ok(())
}

#[tauri::command]
async fn get(state: State<'_, AppState>, key: String) -> Result<(), ()> {
    let mut guard = state.read().await;
    guard.get(&key);

    Ok(())
}

#[tauri::command]
async fn remove(state: State<'_, AppState>, key: String) -> Result<(), ()> {
    let mut guard = state.write().await;
    guard.remove(key).await;

    Ok(())
}

#[tauri::command]
async fn to_hashmap(state: State<'_, AppState>) -> Result<HashMap<String, String>, ()> {
    let mut guard = state.read().await;
    let res = guard.to_hashmap();

    Ok(res)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub async fn run() {
    std::env::set_var("RUST_BACKTRACE", "1");
    std::env::set_var("RUST_LOG", "debug"); // still useful
    let id = 1;

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_blew::init_with_config(
            tauri_plugin_blew::BlewPluginConfig {
                auto_request_permissions: false,
            },
        ))
        .setup(move |app| {
            // Request permissions manually
            if !are_ble_permissions_granted() {
                request_ble_permissions();
                let max_attempts = 300;
                let mut attempts = 0;
                while !are_ble_permissions_granted() && attempts < max_attempts {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    attempts += 1;
                }
                if !are_ble_permissions_granted() {
                    panic!("BLE permissions not granted after waiting");
                }
            }

            let (actor, sender, receiver, router, discovery_handle) =
                futures::executor::block_on(async {
                    groceries_bmesh_core::setup(id).await.unwrap()
                });

            let state = Arc::new(RwLock::new(PeerState::new(actor, sender.clone())));
            app.manage(state.clone());
            app.manage((router, discovery_handle));

            tauri::async_runtime::spawn(groceries_bmesh_core::start_heartbeat_loop(state.clone()));
            tauri::async_runtime::spawn(groceries_bmesh_core::start_respond_loop(
                receiver,
                state.clone(),
            ));

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet, get, update, remove, to_hashmap
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
