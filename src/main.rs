use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::error::Error;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

const CDP_HTTP: &str = "http://127.0.0.1:9222";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    tracing::info!("🚀 Vodafone Idea In-Browser CDP Interceptor & Submitter Started...");

    loop {
        match run_browser_automation().await {
            Ok(_) => {
                tracing::info!("Automation cycle finished. Restarting in 5s...");
                sleep(Duration::from_secs(5)).await;
            }
            Err(e) => {
                tracing::warn!("Session / Connection error: {}. Re-attaching in 2s...", e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn run_browser_automation() -> Result<(), Box<dyn Error>> {
    tracing::info!("📡 Looking for active Vodafone inventory tab in Edge...");

    let client = reqwest::Client::new();
    let targets: Value = client.get(&format!("{}/json", CDP_HTTP)).send().await?.json().await?;

    let arr = targets.as_array().ok_or("No targets found in Edge")?;
    let target = arr.iter().find(|t| {
        let url = t.get("url").and_then(|u| u.as_str()).unwrap_or("");
        url.contains("switchMod.do") || url.contains("cellNumberBlockRelease")
    }).ok_or("Target tab with Vodafone Inventory not open in Edge")?;

    let ws_url = target.get("webSocketDebuggerUrl").and_then(|w| w.as_str()).ok_or("No WebSocket URL")?;
    tracing::info!("🔌 Connecting to WebSocket CDP: {}", ws_url);

    let (ws_stream, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    // 1. Enable Network monitoring and DOM runtime
    let enable_network = serde_json::json!({ "id": 1, "method": "Network.enable" });
    write.send(Message::Text(enable_network.to_string())).await?;

    let enable_page = serde_json::json!({ "id": 2, "method": "Page.enable" });
    write.send(Message::Text(enable_page.to_string())).await?;

    // 2. Execute validateAndSubmit('/cellNumberBlockRelease.do?method=getViewAll') directly in page
    let submit_script = r#"
    (() => {
        const f = document.forms['cellNumberBlockReleaseForm'] || document.forms[0];
        if (!f) return "No Form Found";
        
        // Ensure values are set
        if (f.numberStatus) f.numberStatus.value = "191";
        if (f.cellNumber && !f.cellNumber.value) f.cellNumber.value = "90******98";
        
        console.log("Submitting inventory form...");
        if (typeof window.validateAndSubmit === "function") {
            window.validateAndSubmit('/cellNumberBlockRelease.do?method=getViewAll');
            return "Invoked validateAndSubmit";
        } else {
            f.action = "/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll&entityType=29";
            f.submit();
            return "Submitted form directly";
        }
    })()
    "#;

    tracing::info!("⚡ Triggering in-browser submission from switchMod.do -> getViewAll...");
    let eval_cmd = serde_json::json!({
        "id": 10,
        "method": "Runtime.evaluate",
        "params": {
            "expression": submit_script,
            "returnByValue": true
        }
    });
    write.send(Message::Text(eval_cmd.to_string())).await?;

    // 3. Listen to Network Interception events in real time
    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let res_json: Value = serde_json::from_str(&text)?;
            
            if let Some(method) = res_json.get("method").and_then(|m| m.as_str()) {
                if method == "Network.requestWillBeSent" {
                    let req_url = res_json["params"]["request"]["url"].as_str().unwrap_or("");
                    if req_url.contains("cellNumberBlockRelease") || req_url.contains("getViewAll") {
                        tracing::info!("📡 [Network Intercepted Request]: {}", req_url);
                        if let Some(post_data) = res_json["params"]["request"]["postData"].as_str() {
                            tracing::info!("   Payload: {}", &post_data[..post_data.len().min(120)]);
                        }
                    }
                } else if method == "Network.responseReceived" {
                    let resp_url = res_json["params"]["response"]["url"].as_str().unwrap_or("");
                    let status = res_json["params"]["response"]["status"].as_i64().unwrap_or(0);
                    if resp_url.contains("cellNumberBlockRelease") || resp_url.contains("getViewAll") {
                        tracing::info!("✅ [Network Response Received]: Status {} | {}", status, resp_url);
                    }
                } else if method == "Page.loadEventFired" {
                    tracing::info!("🏁 Page Navigation Completed: Live in Edge!");
                    break;
                }
            }
        }
    }

    Ok(())
}