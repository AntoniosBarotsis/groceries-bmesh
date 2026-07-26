use reqwest::blocking::Client;
use rustls::ClientConfig;
use webpki_roots::TLS_SERVER_ROOTS;

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // std::env::set_var("RUST_BACKTRACE", "full");

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let client = Client::builder()
        .use_preconfigured_tls(config)
        .build()
        .unwrap();

    client
        .get("https://rust-lang.org")
        .send()
        .unwrap()
        .text()
        .unwrap();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
