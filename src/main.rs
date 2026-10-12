use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use regex::Regex;
use serde_json::Value;
use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

const CDP_HTTP: &str = "http://127.0.0.1:9222";
static NEXT_CMD_ID: AtomicI64 = AtomicI64::new(100);

fn get_next_id() -> i64 {
    NEXT_CMD_ID.fetch_add(1, Ordering::SeqCst)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    tracing::info!("🚀 [CDP Ghost Daemon] Initializing event-driven automation...");

    // Master Resilience Loop (State 5)
    loop {
        if let Err(e) = ghost_lifecycle().await {
            tracing::warn!("⚠️ Session disconnect / lifecycle dropped: {}. Resuming State 1 (Discovery)...", e);
            sleep(Duration::from_secs(2)).await;
        } else {
            tracing::info!("Cycle completed normally. Re-entering State 1 in 3s...");
            sleep(Duration::from_secs(3)).await;
        }
    }
}

/// Executes the 5-state CDP Ghost Lifecycle
async fn ghost_lifecycle() -> Result<(), Box<dyn Error>> {
    // ------------------------------------------------------------------------
    // STATE 1: Discovery (Polling for active target tab)
    // ------------------------------------------------------------------------
    tracing::info!("🔍 [State 1: Discovery] Polling Edge for active Vodafone inventory tab...");
    let ws_url = poll_target_tab().await?;
    tracing::info!("🔌 [State 1: Discovered] Target tab found: {}", ws_url);

    // ------------------------------------------------------------------------
    // STATE 2: Attachment & Hydration
    // ------------------------------------------------------------------------
    tracing::info!("🔗 [State 2: Attachment] Connecting to CDP WebSocket...");
    let (ws_stream, _) = connect_async(&ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Enable Network, Page, and Runtime domains
    write.send(Message::Text(serde_json::json!({ "id": 1, "method": "Network.enable" }).to_string())).await?;
    write.send(Message::Text(serde_json::json!({ "id": 2, "method": "Page.enable" }).to_string())).await?;
    write.send(Message::Text(serde_json::json!({ "id": 3, "method": "Runtime.enable" }).to_string())).await?;

    // Hydration check: wait until document.readyState == 'complete'
    tracing::info!("⏳ [State 2: Hydration] Awaiting DOM readiness (document.readyState == 'complete')...");
    wait_for_dom_hydration(&mut write, &mut read).await?;
    tracing::info!("✨ [State 2: Hydration] DOM is fully hydrated!");

    // Human Settling delay: randomized 1.5s - 3.0s
    let mut rng = rand::thread_rng();
    let settling_ms = rng.gen_range(1500..3000);
    tracing::info!("🧘 [State 2: Human Settling] Waiting {}ms to mimic authentic user dwell...", settling_ms);
    sleep(Duration::from_millis(settling_ms)).await;

    // ------------------------------------------------------------------------
    // STATE 3: Injection & Execution
    // ------------------------------------------------------------------------
    tracing::info!("⚡ [State 3: Injection] Triggering form submission via V8 Runtime.evaluate...");
    let submit_script = r#"
    (() => {
        const f = document.forms['cellNumberBlockReleaseForm'] || document.forms[0];
        if (!f) return { status: 'no_form' };

        // Ensure required search filters are set
        if (f.numberStatus) f.numberStatus.value = '191';
        if (f.cellNumber && !f.cellNumber.value) f.cellNumber.value = '90******98';

        const token = f.querySelector("input[name='org.apache.struts.taglib.html.TOKEN']")?.value || '';

        console.log("Ghost Daemon: Executing submission with token", token);
        if (typeof window.validateAndSubmit === 'function') {
            window.validateAndSubmit('/cellNumberBlockRelease.do?method=getViewAll');
            return { status: 'submitted_via_validateAndSubmit', token: token };
        } else {
            f.action = '/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll&entityType=29';
            f.submit();
            return { status: 'submitted_via_form_submit', token: token };
        }
    })()
    "#;

    let eval_id = get_next_id();
    let eval_cmd = serde_json::json!({
        "id": eval_id,
        "method": "Runtime.evaluate",
        "params": {
            "expression": submit_script,
            "returnByValue": true
        }
    });
    write.send(Message::Text(eval_cmd.to_string())).await?;

    // ------------------------------------------------------------------------
    // STATE 4: Interception & Extraction (The 3-Step Hook)
    // ------------------------------------------------------------------------
    tracing::info!("🎯 [State 4: Interception] Arming 3-Step Network hook to capture inventory body...");
    let mut tracked_request_id: Option<String> = None;
    let mut get_body_cmd_id: Option<i64> = None;
    let mut extracted_success = false;

    // Listen to CDP events
    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let res_json: Value = serde_json::from_str(&text)?;

            // Check if this is the response to our Network.getResponseBody command
            if let Some(id) = res_json.get("id").and_then(|i| i.as_i64()) {
                if Some(id) == get_body_cmd_id {
                    tracing::info!("📥 [Step 3/3: getResponseBody] Received body payload from Chromium memory!");
                    if let Some(result) = res_json.get("result") {
                        let body_raw = result.get("body").and_then(|b| b.as_str()).unwrap_or("");
                        let is_base64 = result.get("base64Encoded").and_then(|b| b.as_bool()).unwrap_or(false);

                        let decoded_body = if is_base64 {
                            match base64::engine::general_purpose::STANDARD.decode(body_raw) {
                                Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                                Err(_) => body_raw.to_string(),
                            }
                        } else {
                            body_raw.to_string()
                        };

                        // Extract cell numbers from the HTML body
                        harvest_inventory_results(&decoded_body);
                        extracted_success = true;
                        break;
                    }
                }
            }

            // Handle incoming CDP method notifications
            if let Some(method) = res_json.get("method").and_then(|m| m.as_str()) {
                match method {
                    // Step 1: Capture Request ID
                    "Network.requestWillBeSent" => {
                        let req_url = res_json["params"]["request"]["url"].as_str().unwrap_or("");
                        let req_method = res_json["params"]["request"]["method"].as_str().unwrap_or("");
                        if (req_url.contains("cellNumberBlockRelease") || req_url.contains("getViewAll"))
                            && req_method == "POST"
                        {
                            let req_id = res_json["params"]["requestId"].as_str().unwrap_or("").to_string();
                            tracing::info!("📡 [Step 1/3: requestWillBeSent] Target POST intercepted! RequestId: {}", req_id);
                            if let Some(post_data) = res_json["params"]["request"]["postData"].as_str() {
                                tracing::info!("   Payload: {}", &post_data[..post_data.len().min(100)]);
                            }
                            tracked_request_id = Some(req_id);
                        }
                    }

                    // Step 2: Response Received
                    "Network.responseReceived" => {
                        let req_id = res_json["params"]["requestId"].as_str().unwrap_or("");
                        if Some(req_id) == tracked_request_id.as_deref() {
                            let status = res_json["params"]["response"]["status"].as_i64().unwrap_or(0);
                            let mime = res_json["params"]["response"]["mimeType"].as_str().unwrap_or("");
                            tracing::info!("✅ [Step 2/3: responseReceived] Status: {} | MIME: {}", status, mime);
                        }
                    }

                    // Step 3: Loading Finished -> Trigger Network.getResponseBody
                    "Network.loadingFinished" => {
                        let req_id = res_json["params"]["requestId"].as_str().unwrap_or("");
                        if Some(req_id) == tracked_request_id.as_deref() {
                            tracing::info!("🏁 [Step 3/3: loadingFinished] Download completed! Fetching response body...");
                            let cmd_id = get_next_id();
                            get_body_cmd_id = Some(cmd_id);
                            let body_cmd = serde_json::json!({
                                "id": cmd_id,
                                "method": "Network.getResponseBody",
                                "params": {
                                    "requestId": req_id
                                }
                            });
                            write.send(Message::Text(body_cmd.to_string())).await?;
                        }
                    }

                    "Page.loadEventFired" => {
                        tracing::info!("🌐 [Page Event] Page load event fired in Edge tab.");
                    }

                    _ => {}
                }
            }
        }
    }

    if extracted_success {
        tracing::info!("🎉 [Ghost Cycle Complete] Successfully extracted inventory data without TLS or cookie friction.");
    }

    Ok(())
}

/// Polls http://127.0.0.1:9222/json until a valid inventory tab is open
async fn poll_target_tab() -> Result<String, Box<dyn Error>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    loop {
        match client.get(&format!("{}/json", CDP_HTTP)).send().await {
            Ok(resp) => {
                if let Ok(targets) = resp.json::<Value>().await {
                    if let Some(arr) = targets.as_array() {
                        let target = arr.iter().find(|t| {
                            let url = t.get("url").and_then(|u| u.as_str()).unwrap_or("");
                            url.contains("switchMod.do")
                                || url.contains("cellNumberBlockRelease")
                                || (url.contains("cPOSWeb") && !url.contains("login") && !url.contains("pkms"))
                        });

                        if let Some(t) = target {
                            if let Some(ws_url) = t.get("webSocketDebuggerUrl").and_then(|w| w.as_str()) {
                                return Ok(ws_url.to_string());
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Edge might still be starting
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

/// Executes document.readyState in page context and waits until it returns 'complete'
async fn wait_for_dom_hydration(
    write: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>,
    read: &mut futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..15 {
        let req_id = get_next_id();
        let cmd = serde_json::json!({
            "id": req_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": "document.readyState",
                "returnByValue": true
            }
        });
        write.send(Message::Text(cmd.to_string())).await?;

        // Read response
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if let Message::Text(text) = msg {
                let res: Value = serde_json::from_str(&text)?;
                if res.get("id").and_then(|i| i.as_i64()) == Some(req_id) {
                    let state = res["result"]["result"]["value"].as_str().unwrap_or("");
                    if state == "complete" || state == "interactive" {
                        return Ok(());
                    }
                    break;
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

/// Parses the extracted response body for mobile numbers and table records
fn harvest_inventory_results(html: &str) {
    let re = Regex::new(r"\b([6-9]\d{9})\b").unwrap();
    let mut numbers = HashSet::new();

    for cap in re.captures_iter(html) {
        if let Some(m) = cap.get(1) {
            numbers.insert(m.as_str().to_string());
        }
    }

    if !numbers.is_empty() {
        tracing::info!("========================================================");
        tracing::info!("💎 [INVENTORY HARVEST SUCCESS] Discovered {} Phone Numbers:", numbers.len());
        let mut sorted: Vec<String> = numbers.into_iter().collect();
        sorted.sort();
        for chunk in sorted.chunks(5) {
            tracing::info!("   📱 {}", chunk.join(" | "));
        }
        tracing::info!("========================================================");
    } else {
        tracing::info!("📄 Response payload captured (Length: {} bytes). No numbers matched regex.", html.len());
        if html.contains("CAPTCHA") {
            tracing::warn!("⚠️ Notice: Response contained CAPTCHA challenge requirement.");
        }
    }
}