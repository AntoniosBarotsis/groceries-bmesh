use std::{collections::HashMap, time::Duration};

use futures_timer::Delay;
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos::{ev::SubmitEvent, prelude::*};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"])]
    async fn invoke(cmd: &str, args: JsValue) -> JsValue;
}

#[derive(Debug, Serialize, Deserialize)]
struct InsertArgs {
    key: String,
    value: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct GetArgs {
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoveArgs {
    key: String,
}

#[component]
pub fn App() -> impl IntoView {
    let (name, set_name) = signal(String::new());
    let (dict, set_dict) = signal(String::new());
    let (greet_msg, set_greet_msg) = signal(String::new());

    let update_name = move |ev| {
        let v = event_target_value(&ev);
        set_name.set(v);
    };

    spawn_local(async move {
        loop {
            let msg = invoke("to_hashmap", JsValue::null()).await;
            let msg = serde_wasm_bindgen::from_value::<HashMap<String, String>>(msg).unwrap();
            set_dict.set(format!("{msg:?}"));
            dbg!("yo");
            Delay::new(Duration::from_secs(1)).await;
        }
    });

    let greet = move |ev: SubmitEvent| {
        ev.prevent_default();
        spawn_local(async move {
            let name = name.get_untracked();
            if name.is_empty() {
                return;
            }

            let args = serde_wasm_bindgen::to_value(&InsertArgs {
                key: name,
                value: false,
            })
            .unwrap();
            let _ = invoke("update", args).await;
            let msg = invoke("to_hashmap", JsValue::null()).await;
            let msg = serde_wasm_bindgen::from_value::<HashMap<String, bool>>(msg).unwrap();
            set_dict.set(format!("{msg:?}"))

            // let args = serde_wasm_bindgen::to_value(&GreetArgs { name: &name }).unwrap();
            // // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
            // let new_msg = invoke("greet", args).await.as_string().unwrap();
            // set_greet_msg.set(new_msg);
        });
    };

    view! {
        <main class="container">
            <h1>"Welcome to Tauri + Leptos"</h1>

            <div class="row">
                <a href="https://tauri.app" target="_blank">
                    <img src="public/tauri.svg" class="logo tauri" alt="Tauri logo" />
                </a>
                <a href="https://docs.rs/leptos/" target="_blank">
                    <img src="public/leptos.svg" class="logo leptos" alt="Leptos logo" />
                </a>
            </div>
            <p>"Click on the Tauri and Leptos logos to learn more."</p>

            <form class="row" on:submit=greet>
                <input id="greet-input" placeholder="Enter a name..." on:input=update_name />
                <button type="submit">"Greet"</button>
            </form>
            <p>{dict}</p>
            <p>{move || greet_msg.get()}</p>
        </main>
    }
}
