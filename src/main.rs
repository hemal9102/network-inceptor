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

/// Executes the Fetch-Hijacking CDP Ghost Lifecycle
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

    // Human Settling delay: randomized 1.0s - 2.0s
    let mut rng = rand::thread_rng();
    let settling_ms = rng.gen_range(1000..2000);
    tracing::info!("🧘 [State 2: Human Settling] Waiting {}ms to mimic authentic user dwell...", settling_ms);
    sleep(Duration::from_millis(settling_ms)).await;

    // ------------------------------------------------------------------------
    // STATE 3: Arm the Fetch Interceptor (The Trap)
    // ------------------------------------------------------------------------
    tracing::info!("🕸️ [State 3: Arming Trap] Enabling CDP Fetch domain to hijack POST...");
    let fetch_enable_cmd = serde_json::json!({
        "id": get_next_id(),
        "method": "Fetch.enable",
        "params": {
            "handleAuthRequests": false,
            "patterns": [{
                "urlPattern": "*cellNumberBlockRelease.do*getViewAll*",
                "requestStage": "Request" // Intercept BEFORE it hits the server
            }]
        }
    });
    write.send(Message::Text(fetch_enable_cmd.to_string())).await?;

    // Now trigger the native form submission
    let submit_script = r#"
        (() => {
            const f = document.forms['cellNumberBlockReleaseForm'] || document.forms[0];
            if (f) {
                if (f.numberStatus) f.numberStatus.value = '191';
                if (f.cellNumber && !f.cellNumber.value) f.cellNumber.value = '90******76';
                if (f.entityType) f.entityType.value = '29';
                if (f.entityGroup) f.entityGroup.value = '31157';
                if (f.entity) f.entity.value = '190700217';
                if (f.usertype) f.usertype.value = 'internal';
                if (f.EnttypeId) f.EnttypeId.value = '70';
                if (f.entTypeId) f.entTypeId.value = '70';

                f.action = '/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll&entityType=29';
                f.submit(); // Native submission, but CDP Fetch will catch it!
                return { status: 'submitted' };
            }
            return { status: 'no_form' };
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
    // STATE 4: The Hijack (Catch, Kill, and Fulfill via Rust Reqwest)
    // ------------------------------------------------------------------------
    tracing::info!("⏳ [State 4: Hijack] Waiting for Fetch.requestPaused...");
    let mut cookies_cmd_id: Option<i64> = None;
    let mut hijacked_post_data = String::new();
    let mut hijacked_url = String::new();
    let mut cycle_complete = false;

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let res_json: Value = serde_json::from_str(&text)?;

            // Check if this is the response to Network.getCookies
            if let Some(id) = res_json.get("id").and_then(|i| i.as_i64()) {
                if Some(id) == cookies_cmd_id {
                    tracing::info!("🍪 [Cookies Received] Extracting browser session cookies...");
                    let mut cookie_header = String::new();

                    if let Some(cookies) = res_json.get("result").and_then(|r| r.get("cookies")).and_then(|c| c.as_array()) {
                        for c in cookies {
                            if let (Some(name), Some(value)) = (c.get("name").and_then(|n| n.as_str()), c.get("value").and_then(|v| v.as_str())) {
                                if !cookie_header.is_empty() {
                                    cookie_header.push_str("; ");
                                }
                                cookie_header.push_str(&format!("{}={}", name, value));
                            }
                        }
                    }

                    tracing::info!("🔑 Session Cookie Header built (Length: {} bytes)", cookie_header.len());

                    // Fire Rust Reqwest in background using browser's cookies and intercepted payload
                    tracing::info!("🚀 [Rust Reqwest] Firing background POST with intercepted payload...");
                    let client = reqwest::Client::builder()
                        .timeout(Duration::from_secs(15))
                        .build()?;

                    let target_url = if hijacked_url.is_empty() {
                        "https://cpos3.vodafoneidea.com/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll&entityType=29".to_string()
                    } else {
                        hijacked_url.clone()
                    };

                    let real_response = client.post(&target_url)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("Cookie", &cookie_header)
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")
                        .header("Origin", "https://cpos3.vodafoneidea.com")
                        .header("Referer", "https://cpos3.vodafoneidea.com/cPOSWeb/jsp/inventory/switchMod.do?prefix=/jsp/inventory&page=/cellNumberBlockRelease.do?method=getView&fromMenu=Y")
                        .body(hijacked_post_data.clone())
                        .send()
                        .await?;

                    let status = real_response.status();
                    tracing::info!("📡 Reqwest response status: {}", status);
                    let html_body = real_response.text().await?;

                    // Harvest
                    if is_results_table(&html_body) {
                        tracing::info!("💎 [Verified Results Table] Confirmed via checkedArray!");
                        harvest_inventory_results(&html_body);
                    } else if is_captcha_challenge_page(&html_body) {
                        tracing::warn!("🧩 Challenge page returned. Captcha validation required on response.");
                    } else {
                        tracing::info!("📄 Response payload captured (Length: {} bytes).", html_body.len());
                    }

                    cycle_complete = true;
                    break;
                }
            }

            // Check CDP method events
            if let Some(method) = res_json.get("method").and_then(|m| m.as_str()) {
                if method == "Fetch.requestPaused" {
                    let req_id = res_json["params"]["requestId"].as_str().unwrap_or("").to_string();
                    let post_data = res_json["params"]["request"]["postData"].as_str().unwrap_or("");
                    let req_url = res_json["params"]["request"]["url"].as_str().unwrap_or("");

                    tracing::info!("🛑 [HIJACKED] Native POST caught! Pausing navigation...");
                    tracing::info!("   URL: {}", req_url);
                    if !post_data.is_empty() {
                        tracing::info!("   Payload: {}", &post_data[..post_data.len().min(100)]);
                    }

                    hijacked_post_data = post_data.to_string();
                    hijacked_url = req_url.to_string();

                    // 1. KILL the browser's request so the page DOES NOT NAVIGATE
                    let kill_cmd = serde_json::json!({
                        "id": get_next_id(),
                        "method": "Fetch.failRequest",
                        "params": {
                            "requestId": req_id,
                            "errorReason": "Aborted"
                        }
                    });
                    write.send(Message::Text(kill_cmd.to_string())).await?;

                    // 2. Extract Cookies from browser via Network.getCookies
                    let cmd_id = get_next_id();
                    cookies_cmd_id = Some(cmd_id);
                    let cookies_cmd = serde_json::json!({
                        "id": cmd_id,
                        "method": "Network.getCookies",
                        "params": {
                            "urls": ["https://cpos3.vodafoneidea.com"]
                        }
                    });
                    write.send(Message::Text(cookies_cmd.to_string())).await?;
                }
            }
        }
    }

    // Disable Fetch domain so future tabs/pages aren't indefinitely paused
    let disable_fetch_cmd = serde_json::json!({
        "id": get_next_id(),
        "method": "Fetch.disable"
    });
    let _ = write.send(Message::Text(disable_fetch_cmd.to_string())).await;

    if cycle_complete {
        tracing::info!("🎉 [Ghost Cycle Complete] Fetch hijack and background reqwest finished successfully.");
    }

    Ok(())
}

/// Verifies whether the HTML is the actual inventory results table
fn is_results_table(html: &str) -> bool {
    html.contains("checkedArray") || html.contains("name=\"checkedArray\"")
}

/// Verifies if we were redirected/kept on the CAPTCHA challenge page
fn is_captcha_challenge_page(html: &str) -> bool {
    html.contains("name=\"captcha1\"") && html.contains("name=\"captcha\"")
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
    }
}
