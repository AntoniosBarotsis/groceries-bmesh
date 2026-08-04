use std::{collections::HashMap, time::Duration};

use futures_timer::Delay;
use leptos::ev::SubmitEvent;
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos::web_sys::console;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use js_sys::Function;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"])]
    async fn invoke(cmd: &str, args: JsValue) -> JsValue;

    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "event"])]
    fn listen(event: &str, handler: &Function) -> js_sys::Promise;
}

#[derive(Debug, Serialize, Deserialize)]
struct InsertArgs {
    key: String,
    value: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoveArgs {
    key: String,
}

#[component]
pub fn App() -> impl IntoView {
    let (groceries, set_groceries) = signal(Vec::<(String, bool)>::new());
    let (new_grocery, set_new_grocery) = signal(String::new());

    let (editing_key, set_editing_key) = signal(Option::<String>::None);
    let (edit_text, set_edit_text) = signal(String::new());

    let polling_started = RwSignal::new(false);
    let (loading, set_loading) = signal(true);
    let (peers_connected, set_peers_connected) = signal(0);

    async fn fetch_groceries(set_groceries: WriteSignal<Vec<(String, bool)>>) {
        let msg = invoke("to_hashmap", JsValue::null()).await;
        match serde_wasm_bindgen::from_value::<HashMap<String, String>>(msg) {
            Ok(map) => {
                let mut vec: Vec<(String, bool)> = map
                    .into_iter()
                    .map(|(k, v)| (k, v.parse::<bool>().expect("Could not parse V")))
                    .collect();
                vec.sort_by(|a, b| a.0.cmp(&b.0));
                set_groceries.set(vec);
            }
            Err(e) => {
                console::error_1(&format!("Failed to deserialize groceries: {:?}", e).into());
            }
        }
    }

    async fn start_background() -> Result<(), String> {
        let _ = invoke("start_background_tasks", JsValue::null()).await;
        Ok(())
    }

    fn start_polling(set_groceries: WriteSignal<Vec<(String, bool)>>, started: RwSignal<bool>) {
        if started.get() {
            return;
        }
        started.set(true);
        spawn_local(async move {
            loop {
                fetch_groceries(set_groceries).await;
                Delay::new(Duration::from_millis(100)).await;
            }
        });
    }

    async fn wait_for_backend() {
        loop {
            let ready = leptos::web_sys::window()
                .and_then(|w| {
                    js_sys::Reflect::get(&w, &JsValue::from_str("__BACKEND_READY__")).ok()
                })
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(false);

            if ready {
                break;
            }
            Delay::new(Duration::from_millis(100)).await;
        }
    }

    spawn_local({
        async move {
            console::log_1(&"Waiting for backend...".into());
            wait_for_backend().await;
            console::log_1(&"Backend ready, proceeding...".into());

            let _ = invoke("load_state", JsValue::null()).await;
            let _ = start_background().await;
            start_polling(set_groceries, polling_started);
            set_loading.set(false);

            spawn_local(async move {
                loop {
                    let msg = invoke("get_peers_connected", JsValue::null()).await;
                    if let Ok(n) = serde_wasm_bindgen::from_value::<usize>(msg) {
                        set_peers_connected.set(n);
                    }
                    Delay::new(Duration::from_secs(1)).await;
                }
            });
        }
    });

    let save_state = move |_| {
        spawn_local(async move {
            let _ = invoke("save_state", JsValue::null()).await;
        });
    };

    let clear_all = move |_| {
        spawn_local(async move {
            let _ = invoke("clear", JsValue::null()).await;
            let _ = invoke("save_state", JsValue::null()).await;
        });
    };

    let add_grocery = move |ev: SubmitEvent| {
        ev.prevent_default();
        let text = new_grocery.get_untracked();
        if text.is_empty() {
            return;
        }
        spawn_local(async move {
            let args = serde_wasm_bindgen::to_value(&InsertArgs {
                key: text.clone(),
                value: false,
            })
            .unwrap();
            let _ = invoke("update_value", args).await;
        });
        set_new_grocery.set(String::new());
    };

    let toggle_grocery = move |key: String, current: bool| {
        console::log_1(&format!("Toggling {} from {}", key, current).into());
        spawn_local(async move {
            let args = serde_wasm_bindgen::to_value(&InsertArgs {
                key: key.clone(),
                value: !current,
            })
            .unwrap();
            let _ = invoke("update_value", args).await;
        });
    };

    let delete_grocery = move |key: String| {
        // If we delete the item currently being edited, clear editing state
        if editing_key.get_untracked() == Some(key.clone()) {
            set_editing_key.set(None);
        }
        spawn_local(async move {
            let args = serde_wasm_bindgen::to_value(&RemoveArgs { key: key.clone() }).unwrap();
            let _ = invoke("remove", args).await;
        });
    };

    let start_edit = move |key: String| {
        if editing_key.get_untracked() == Some(key.clone()) {
            return;
        }
        set_editing_key.set(Some(key.clone()));
        set_edit_text.set(key);
    };

    let finish_edit = move |old_key: String, current_value: bool| {
        // Guard: if we've already moved on to editing something else, do nothing
        if editing_key.get_untracked() != Some(old_key.clone()) {
            return;
        }
        let new_key = edit_text.get_untracked();
        set_editing_key.set(None);
        if new_key.is_empty() || new_key == old_key {
            return;
        }
        spawn_local(async move {
            let remove_args = serde_wasm_bindgen::to_value(&RemoveArgs {
                key: old_key.clone(),
            })
            .unwrap();
            let _ = invoke("remove", remove_args).await;

            let insert_args = serde_wasm_bindgen::to_value(&InsertArgs {
                key: new_key,
                value: current_value,
            })
            .unwrap();
            let _ = invoke("update_value", insert_args).await;
        });
    };

    let on_input = move |ev| {
        set_new_grocery.set(event_target_value(&ev));
    };

    view! {
        <main class="container">
            {move || {
                if loading.get() {
                    view! {
                        <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; gap: 1rem;">
                            <div style="
                                width: 40px;
                                height: 40px;
                                border: 4px solid #ccc;
                                border-top-color: #333;
                                border-radius: 50%;
                                animation: spin 1s linear infinite;
                            "></div>
                            <p style="color: #666; font-size: 0.9rem;">"Loading…"</p>
                        </div>
                        <style>"
                            @keyframes spin {
                                to { transform: rotate(360deg); }
                            }
                        "</style>
                    }
                        .into_any()
                } else {
                    view! {
                        <div>
                            <h1>"📋 Grocery List"</h1>
                            <div style="display: flex; justify-content: flex-end; align-items: baseline; margin-bottom: 0.5rem;">
                                <span style="font-size: 0.85rem; color: #666;">
                                    {move || {
                                        let n = peers_connected.get();
                                        if n == 1 {
                                            "👥 1 peer".to_string()
                                        } else {
                                            format!("👥 {} peers", n)
                                        }
                                    }}
                                </span>
                            </div>

                            <form
                                class="row"
                                on:submit=add_grocery
                                style="gap: 1rem; display: flex; width: 100%; align-items: center; margin-bottom: 1rem;"
                            >
                                <input
                                    id="new-grocery-input"
                                    placeholder="Add a new item..."
                                    on:input=on_input
                                    prop:value=move || new_grocery.get()
                                    style="flex: 1; min-width: 0;"
                                />
                                <button type="submit">"Add"</button>
                            </form>

                            <ul style="display: flex; flex-direction: column; gap: 0.75rem; padding: 0; margin: 0; list-style: none;">
                                {move || {
                                    groceries
                                        .get()
                                        .into_iter()
                                        .map(|(text, done)| {
                                            let text_for_disabled = text.clone();
                                            let text_for_toggle = text.clone();
                                            let text_for_blur = text.clone();
                                            let text_for_keydown = text.clone();
                                            let text_for_input_display = text.clone();
                                            let text_for_click = text.clone();
                                            let text_for_span_display = text.clone();
                                            let text_for_delete = text.clone();

                                            view! {
                                                <li style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                                                    <span style="display: flex; align-items: center; flex: 1; min-width: 0;">
                                                        <input
                                                            type="checkbox"
                                                            prop:checked=done
                                                            prop:disabled=move || {
                                                                editing_key.get() == Some(text_for_disabled.clone())
                                                            }
                                                            on:change=move |_| toggle_grocery(
                                                                text_for_toggle.clone(),
                                                                done,
                                                            )
                                                        />
                                                        <input
                                                            type="text"
                                                            prop:value=move || edit_text.get()
                                                            on:input=move |ev| {
                                                                set_edit_text.set(event_target_value(&ev))
                                                            }
                                                            on:blur=move |_| finish_edit(text_for_blur.clone(), done)
                                                            on:keydown=move |ev| {
                                                                if ev.key() == "Enter" {
                                                                    finish_edit(text_for_keydown.clone(), done);
                                                                } else if ev.key() == "Escape" {
                                                                    set_editing_key.set(None);
                                                                }
                                                            }
                                                            style:display=move || {
                                                                if editing_key.get() == Some(text_for_input_display.clone())
                                                                {
                                                                    "block"
                                                                } else {
                                                                    "none"
                                                                }
                                                            }
                                                            style="margin-left: 0.5rem; flex: 1; min-width: 0; font-size: 1rem;"
                                                        />
                                                        <span
                                                            style:color=move || if done { "gray" } else { "inherit" }
                                                            on:click=move |ev| {
                                                                start_edit(text_for_click.clone());
                                                                if let Some(target) = ev.current_target() {
                                                                    if let Some(el) = target
                                                                        .dyn_ref::<leptos::web_sys::Element>()
                                                                    {
                                                                        if let Some(prev) = el.previous_element_sibling() {
                                                                            if let Ok(input) = prev
                                                                                .dyn_into::<leptos::web_sys::HtmlInputElement>()
                                                                            {
                                                                                let _ = input.focus();
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            style:display=move || {
                                                                if editing_key.get() == Some(text_for_span_display.clone())
                                                                {
                                                                    "none"
                                                                } else {
                                                                    "block"
                                                                }
                                                            }
                                                            style="margin-left: 0.5rem; cursor: pointer; user-select: none; text-decoration: underline; text-decoration-style: dotted; text-decoration-color: #aaa; overflow-wrap: break-word; word-break: break-word; white-space: normal; flex: 1; min-width: 0; text-align: left;"
                                                        >
                                                            {text}
                                                        </span>
                                                    </span>
                                                    <button
                                                        on:click=move |_| delete_grocery(text_for_delete.clone())
                                                        style="margin-left: 10px; flex-shrink: 0;"
                                                    >
                                                        "❌"
                                                    </button>
                                                </li>
                                            }
                                        })
                                        .collect::<Vec<_>>()
                                }}
                            </ul>

                            <p>
                                {move || {
                                    let remaining = groceries.get().iter().filter(|(_, done)| !done).count();
                                    format!("{} remaining items", remaining)
                                }}
                            </p>

                            <div style="display: flex; gap: 0.5rem; margin-top: 1rem; justify-content: center;">
                                <button on:click=save_state>"💾 Save"</button>
                                <button on:click=clear_all>"🗑️ Clear All"</button>
                            </div>
                        </div>
                    }
                        .into_any()
                }
            }}
        </main>
    }
}
