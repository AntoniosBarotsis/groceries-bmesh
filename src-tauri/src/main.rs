// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[tokio::main]
async fn main() {
    tauri::async_runtime::set(tokio::runtime::Handle::current());

    groceries_bmesh_lib::run().await
}
