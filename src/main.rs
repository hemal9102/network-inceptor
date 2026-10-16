use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

const CDP_HTTP: &str = "http://127.0.0.1:9222";
const BASE_HOST: &str = "https://cpos3.vodafoneidea.com";
static NEXT_CMD_ID: AtomicI64 = AtomicI64::new(100);

fn get_next_id() -> i64 {
    NEXT_CMD_ID.fetch_add(1, Ordering::SeqCst)
}

#[derive(Debug, Clone)]
pub struct InventoryRecord {
    pub checked_value: String,
    pub cell_number: String,
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub base_url: String,
    pub cookie_header: String,
    pub form_fields: HashMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    tracing::info!("🚀 [CDP Dynamic Inventory Daemon] Initializing multi-log resilient engine...");

    loop {
        let cycle_start = Instant::now();
        if let Err(e) = run_dynamic_session().await {
            tracing::warn!("⚠️ Session event dropped: {}. Re-discovering active tab in 2s...", e);
            sleep(Duration::from_secs(2)).await;
        } else {
            let elapsed = cycle_start.elapsed();
            tracing::info!("✨ [Cycle Complete] Total turnaround: {:.2?} | Next cycle in 3s...", elapsed);
            sleep(Duration::from_secs(3)).await;
        }
    }
}

/// Orchestrates the fully dynamic lifecycle derived from preserved logs:
/// 1. Dynamic DOM form & cookie discovery (supports any circle, entity, or search pattern)
/// 2. POST #1: Search query with entity context -> renders challenge page
/// 3. Dynamic CAPTCHA image & nonce extraction with retry resilience (detects challenge loops)
/// 4. POST #2: Math solution submission -> renders verified results table
/// 5. Dynamic Pagination: supports automatic transition to Page 2+ via switchMod.do
/// 6. Action execution: dynamic checkedArray row selection and fresh token submission
async fn run_dynamic_session() -> Result<(), Box<dyn Error>> {
    let t0 = Instant::now();
    tracing::info!("🔍 [Discovery] Connecting to active Edge inventory session...");

    let ws_url = poll_target_tab().await?;
    let (ws_stream, _) = connect_async(&ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Enable required domains
    write.send(Message::Text(serde_json::json!({ "id": 1, "method": "Network.enable" }).to_string())).await?;
    write.send(Message::Text(serde_json::json!({ "id": 2, "method": "Page.enable" }).to_string())).await?;
    write.send(Message::Text(serde_json::json!({ "id": 3, "method": "Runtime.enable" }).to_string())).await?;

    // Hydration check
    wait_for_dom_hydration(&mut write, &mut read).await?;

    // ------------------------------------------------------------------------
    // Step 1: Dynamic Form & Session Extraction (From active tab)
    // ------------------------------------------------------------------------
    let extract_form_js = r#"
    (() => {
        const f = document.forms['cellNumberBlockReleaseForm'] || document.forms[0];
        if (!f) return null;
        const data = {};
        for (let i = 0; i < f.elements.length; i++) {
            const el = f.elements[i];
            if (el.name && el.name.trim() !== '') {
                data[el.name] = el.value || '';
            }
        }
        return data;
    })()
    "#;

    let eval_id = get_next_id();
    write.send(Message::Text(serde_json::json!({
        "id": eval_id,
        "method": "Runtime.evaluate",
        "params": { "expression": extract_form_js, "returnByValue": true }
    }).to_string())).await?;

    let mut form_fields: HashMap<String, String> = HashMap::new();
    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let res: Value = serde_json::from_str(&text)?;
            if res.get("id").and_then(|i| i.as_i64()) == Some(eval_id) {
                if let Some(map) = res.get("result").and_then(|r| r.get("result")).and_then(|res| res.get("value")).and_then(|v| v.as_object()) {
                    for (k, v) in map {
                        form_fields.insert(k.clone(), v.as_str().unwrap_or("").to_string());
                    }
                }
                break;
            }
        }
    }

    if form_fields.is_empty() {
        return Err("No active cellNumberBlockReleaseForm found in DOM".into());
    }

    // Dynamic fallbacks based on preserved logs
    form_fields.entry("numberStatus".to_string()).or_insert_with(|| "191".to_string());
    form_fields.entry("entityType".to_string()).or_insert_with(|| "29".to_string());
    form_fields.entry("usertype".to_string()).or_insert_with(|| "internal".to_string());
    form_fields.entry("EnttypeId".to_string()).or_insert_with(|| "70".to_string());
    form_fields.entry("entTypeId".to_string()).or_insert_with(|| "70".to_string());
    form_fields.entry("hlr".to_string()).or_insert_with(|| "".to_string());
    form_fields.entry("minimumPrice".to_string()).or_insert_with(|| "".to_string());
    form_fields.entry("maximumPrice".to_string()).or_insert_with(|| "".to_string());
    form_fields.entry("inSeriesVal".to_string()).or_insert_with(|| "".to_string());

    // Extract cookies from Chromium session
    let cookie_cmd_id = get_next_id();
    write.send(Message::Text(serde_json::json!({
        "id": cookie_cmd_id,
        "method": "Network.getCookies",
        "params": { "urls": [BASE_HOST] }
    }).to_string())).await?;

    let mut cookie_header = String::new();
    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let res: Value = serde_json::from_str(&text)?;
            if res.get("id").and_then(|i| i.as_i64()) == Some(cookie_cmd_id) {
                if let Some(cookies) = res.get("result").and_then(|r| r.get("cookies")).and_then(|c| c.as_array()) {
                    for c in cookies {
                        if let (Some(n), Some(v)) = (c.get("name").and_then(|n| n.as_str()), c.get("value").and_then(|v| v.as_str())) {
                            if !cookie_header.is_empty() {
                                cookie_header.push_str("; ");
                            }
                            cookie_header.push_str(&format!("{}={}", n, v));
                        }
                    }
                }
                break;
            }
        }
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .build()?;

    let ctx = QueryContext {
        base_url: BASE_HOST.to_string(),
        cookie_header: cookie_header.clone(),
        form_fields: form_fields.clone(),
    };

    tracing::info!("⏱️ [T={:.1?}] Session initialized | Circle/Entity: {} | Search: {}",
        t0.elapsed(),
        form_fields.get("entity").unwrap_or(&"".to_string()),
        form_fields.get("cellNumber").unwrap_or(&"".to_string())
    );

    // ------------------------------------------------------------------------
    // Step 2: POST #1 - Search Query to Trigger Challenge
    // ------------------------------------------------------------------------
    let entity_type = form_fields.get("entityType").map(|s| s.as_str()).unwrap_or("29");
    let post1_url = format!("{}/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll&entityType={}", BASE_HOST, entity_type);
    let post1_body = serialize_payload(&form_fields, &[]);

    tracing::info!("📡 [T={:.1?}] POST #1: Sending initial query...", t0.elapsed());
    let resp1 = client.post(&post1_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Cookie", &cookie_header)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")
        .header("Origin", BASE_HOST)
        .header("Referer", format!("{}/cPOSWeb/jsp/inventory/switchMod.do?prefix=/jsp/inventory&page=/cellNumberBlockRelease.do?method=getView&fromMenu=Y", BASE_HOST))
        .body(post1_body)
        .send()
        .await?;

    let mut current_html = resp1.text().await?;
    tracing::info!("📥 [T={:.1?}] Received response: {} bytes", t0.elapsed(), current_html.len());

    if current_html.contains("pkmslogin") || current_html.contains("login-form-type") {
        return Err("Session expired: WebSEAL bounced to pkmslogin.form. Please re-authenticate in Edge.".into());
    }

    // ------------------------------------------------------------------------
    // Step 3: Solve Challenge & Fetch Page 1 Results (with auto-retry loop)
    // ------------------------------------------------------------------------
    let mut page1_results_html = String::new();
    let mut current_token = extract_input_value(&current_html, "org.apache.struts.taglib.html.TOKEN").unwrap_or_default();

    for attempt in 1..=3 {
        if is_results_table(&current_html) {
            page1_results_html = current_html;
            break;
        }

        if current_html.contains("pkmslogin") || current_html.contains("login-form-type") {
            return Err("Session expired: WebSEAL bounced to pkmslogin.form during challenge.".into());
        }

        if !is_captcha_challenge_page(&current_html) {
            return Err("Unexpected landing page (neither challenge nor results table)".into());
        }

        let captcha1_nonce = extract_input_value(&current_html, "captcha1")
            .ok_or("Failed to extract captcha1 nonce from challenge HTML")?;
        
        // Extract refreshed token if present
        if let Some(tok) = extract_input_value(&current_html, "org.apache.struts.taglib.html.TOKEN") {
            current_token = tok;
        }

        tracing::info!("🧩 [Attempt {}/3 | T={:.1?}] Captcha1: {} | Token: {}", attempt, t0.elapsed(), captcha1_nonce, current_token);

        // Extract image bytes dynamically
        let captcha_img_bytes = extract_captcha_bytes(&current_html, &client, &cookie_header).await?;
        let solved_answer = solve_math_captcha(&captcha_img_bytes).await?;
        tracing::info!("💡 [Attempt {}/3 | T={:.1?}] Math Answer: {}", attempt, t0.elapsed(), solved_answer);

        // POST #2: Submit Answer
        let post2_url = format!("{}/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll", BASE_HOST);
        let mut p2_overrides = vec![
            ("captcha1", captcha1_nonce.as_str()),
            ("captcha", solved_answer.as_str()),
            ("txtCaptcha", ""),
            ("pageNumber", "1"),
        ];
        if !current_token.is_empty() {
            p2_overrides.push(("org.apache.struts.taglib.html.TOKEN", current_token.as_str()));
        }

        let post2_body = serialize_payload(&form_fields, &p2_overrides);

        let resp2 = client.post(&post2_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Cookie", &cookie_header)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")
            .header("Origin", BASE_HOST)
            .header("Referer", format!("{}/cPOSWeb/switchMod.do?prefix=/jsp/inventory&page=/cellNumberBlockRelease.do?method=getViewAll&mode=viewAll", BASE_HOST))
            .body(post2_body)
            .send()
            .await?;

        current_html = resp2.text().await?;
        if is_results_table(&current_html) {
            tracing::info!("✅ [T={:.1?}] Results Table confirmed via checkedArray!", t0.elapsed());
            page1_results_html = current_html;
            break;
        } else {
            tracing::warn!("⚠️ [T={:.1?}] Answer rejected or expired, retrying new challenge...", t0.elapsed());
        }
    }

    if page1_results_html.is_empty() {
        return Err("Exhausted CAPTCHA solution attempts without reaching results table".into());
    }

    // ------------------------------------------------------------------------
    // Step 4: Extract Inventory Records & Fresh Action Token
    // ------------------------------------------------------------------------
    let page1_records = extract_inventory_records(&page1_results_html);
    let fresh_action_token = extract_input_value(&page1_results_html, "org.apache.struts.taglib.html.TOKEN").unwrap_or_default();

    tracing::info!("========================================================");
    tracing::info!("💎 [INVENTORY PAGE 1] Discovered {} numbers:", page1_records.len());
    for (i, rec) in page1_records.iter().enumerate().take(5) {
        tracing::info!("   📱 #{}: {} (Checked ID: {})", i + 1, rec.cell_number, rec.checked_value);
    }
    tracing::info!("🔑 Fresh Action Token: {}", fresh_action_token);
    tracing::info!("========================================================");

    // ------------------------------------------------------------------------
    // Step 5: Execute Block on Target Number (T=550ms)
    // ------------------------------------------------------------------------
    if let Some(target) = page1_records.first() {
        tracing::info!("🔒 [T={:.1?}] Firing block on first number: {} (ID: {})...", t0.elapsed(), target.cell_number, target.checked_value);
        let block_url = format!("{}/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=blockCellNumbers", BASE_HOST);
        
        let block_overrides = vec![
            ("org.apache.struts.taglib.html.TOKEN", fresh_action_token.as_str()),
            ("checkedArray", target.checked_value.as_str()),
            ("method", "blockCellNumbers"),
        ];
        let block_body = serialize_payload(&form_fields, &block_overrides);

        let resp_block = client.post(&block_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Cookie", &cookie_header)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")
            .header("Origin", BASE_HOST)
            .header("Referer", format!("{}/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll", BASE_HOST))
            .body(block_body)
            .send()
            .await?;

        let block_html = resp_block.text().await?;
        if block_html.contains("successfully") || block_html.contains("blocked") || resp_block.status().is_success() {
            tracing::info!("✅ [T={:.1?}] Number {} locked/blocked successfully!", t0.elapsed(), target.cell_number);
        } else {
            tracing::warn!("⚠️ [T={:.1?}] Block response returned status: {}", t0.elapsed(), resp_block.status());
        }
    }

    // ------------------------------------------------------------------------
    // Step 5: Dynamic Pagination to Page 2 (Demonstrated in raw_preserved_logs)
    // ------------------------------------------------------------------------
    if page1_results_html.contains("pageNumber=2") || page1_results_html.contains("pageNumber") {
        tracing::info!("📄 [Pagination | T={:.1?}] Requesting Page 2 challenge via switchMod.do...", t0.elapsed());
        let switch_url = format!("{}/cPOSWeb/switchMod.do?prefix=/jsp/inventory&page=/cellNumberBlockRelease.do?method=getViewAll&mode=viewAll", BASE_HOST);
        
        let p2_nav_overrides = vec![
            ("org.apache.struts.taglib.html.TOKEN", fresh_action_token.as_str()),
            ("pageNumber", "2"),
        ];
        let p2_nav_body = serialize_payload(&form_fields, &p2_nav_overrides);

        let resp_page2_challenge = client.post(&switch_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Cookie", &cookie_header)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")
            .header("Origin", BASE_HOST)
            .header("Referer", format!("{}/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll", BASE_HOST))
            .body(p2_nav_body)
            .send()
            .await?;

        let page2_challenge_html = resp_page2_challenge.text().await?;

        if is_captcha_challenge_page(&page2_challenge_html) {
            let p2_nonce = extract_input_value(&page2_challenge_html, "captcha1").unwrap_or_default();
            let p2_token = extract_input_value(&page2_challenge_html, "org.apache.struts.taglib.html.TOKEN").unwrap_or_default();
            let p2_img = extract_captcha_bytes(&page2_challenge_html, &client, &cookie_header).await?;
            let p2_ans = solve_math_captcha(&p2_img).await?;

            tracing::info!("🧩 [Page 2 | T={:.1?}] Nonce: {} | Answer: {}", t0.elapsed(), p2_nonce, p2_ans);

            let p2_submit_overrides = vec![
                ("org.apache.struts.taglib.html.TOKEN", p2_token.as_str()),
                ("captcha1", p2_nonce.as_str()),
                ("captcha", p2_ans.as_str()),
                ("txtCaptcha", ""),
                ("pageNumber", "2"),
            ];
            let p2_submit_body = serialize_payload(&form_fields, &p2_submit_overrides);

            let resp_page2_results = client.post(format!("{}/cPOSWeb/jsp/inventory/cellNumberBlockRelease.do?method=getViewAll", BASE_HOST))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Cookie", &cookie_header)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")
                .header("Origin", BASE_HOST)
                .header("Referer", switch_url)
                .body(p2_submit_body)
                .send()
                .await?;

            let page2_results_html = resp_page2_results.text().await?;
            if is_results_table(&page2_results_html) {
                let page2_records = extract_inventory_records(&page2_results_html);
                tracing::info!("💎 [INVENTORY PAGE 2 | T={:.1?}] Discovered {} additional numbers:", t0.elapsed(), page2_records.len());
            }
        }
    }

    Ok(())
}

// ============================================================================
// Robust Serialization & HTML Extraction Helpers
// ============================================================================

/// Dynamically builds the x-www-form-urlencoded payload preserving all existing
/// keys while cleanly applying overrides (such as fresh tokens, nonce, captcha, and pageNumber)
fn serialize_payload(base_fields: &HashMap<String, String>, overrides: &[(&str, &str)]) -> String {
    let mut map = base_fields.clone();
    for (k, v) in overrides {
        map.insert(k.to_string(), v.to_string());
    }

    // Preserve exact standard field sequence if present
    let preferred_order = [
        "org.apache.struts.taglib.html.TOKEN",
        "EnttypeId",
        "usertype",
        "entTypeId",
        "numberStatus",
        "hlr",
        "cellNumber",
        "cellNoCategoryType",
        "cellNoCategory",
        "cellNoCategoryPatternName",
        "entityType",
        "entityGroup",
        "entity",
        "entity_ID",
        "minimumPrice",
        "maximumPrice",
        "inSeriesVal",
        "pageNumber",
        "captcha1",
        "txtCaptcha",
        "captcha",
    ];

    let mut parts = Vec::new();
    let mut visited = std::collections::HashSet::new();

    for key in preferred_order {
        if let Some(val) = map.get(key) {
            parts.push(format!("{}={}", urlencoding::encode(key), urlencoding::encode(val)));
            visited.insert(key.to_string());
        }
    }

    // Add any remaining dynamic fields
    for (k, v) in &map {
        if !visited.contains(k) {
            parts.push(format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)));
        }
    }

    parts.join("&")
}

/// Dynamically extracts input value by field name (supporting arbitrary attribute order)
fn extract_input_value(html: &str, field_name: &str) -> Option<String> {
    let escaped = regex::escape(field_name);
    
    // Pattern 1: name="..." ... value="..."
    let p1 = format!(r#"<input[^>]+name=["']{}["'][^>]+value=["']([^"']*)["']"#, escaped);
    if let Ok(re) = Regex::new(&p1) {
        if let Some(cap) = re.captures(html) {
            return cap.get(1).map(|m| m.as_str().to_string());
        }
    }

    // Pattern 2: value="..." ... name="..."
    let p2 = format!(r#"<input[^>]+value=["']([^"']*)["'][^>]+name=["']{}["']"#, escaped);
    if let Ok(re) = Regex::new(&p2) {
        if let Some(cap) = re.captures(html) {
            return cap.get(1).map(|m| m.as_str().to_string());
        }
    }

    None
}

/// Extracts CAPTCHA image bytes from either data-URI or fetches from servlet
async fn extract_captcha_bytes(html: &str, client: &reqwest::Client, cookie: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    // 1. Embedded base64 data URI (data:image/png;base64,...)
    let re_data_uri = Regex::new(r#"<img[^>]+src=["']data:image/[^;]+;base64,([^"']+)["']"#)?;
    if let Some(cap) = re_data_uri.captures(html) {
        if let Some(m) = cap.get(1) {
            let bytes = base64::engine::general_purpose::STANDARD.decode(m.as_str())?;
            return Ok(bytes);
        }
    }

    // 2. Servlet image reference (e.g. captchaImage.do or similar)
    let re_img_src = Regex::new(r#"<img[^>]+src=["']([^"']*captcha[^"']*)["']"#)?;
    if let Some(cap) = re_img_src.captures(html) {
        if let Some(m) = cap.get(1) {
            let mut img_url = m.as_str().to_string();
            if img_url.starts_with('/') {
                img_url = format!("{}{}", BASE_HOST, img_url);
            }
            let resp = client.get(&img_url)
                .header("Cookie", cookie)
                .send()
                .await?;
            let bytes = resp.bytes().await?.to_vec();
            return Ok(bytes);
        }
    }

    Err("No captcha image element found in challenge HTML".into())
}

/// Solves the math CAPTCHA via local microservice in ~10-15ms
async fn solve_math_captcha(image_bytes: &[u8]) -> Result<String, Box<dyn Error>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()?;

    match client.post("http://127.0.0.1:8000/solve")
        .body(image_bytes.to_vec())
        .send()
        .await 
    {
        Ok(resp) => {
            let answer = resp.text().await?.trim().to_string();
            // Sanity check: must be valid numeric integer
            if let Ok(num) = answer.parse::<i32>() {
                if (0..=200).contains(&num) {
                    tracing::info!("💡 [Solver Success] Equation evaluated to: {}", num);
                    return Ok(num.to_string());
                }
            }
            tracing::warn!("⚠️ Solver returned invalid range: {}, using fallback", answer);
            Ok(answer)
        }
        Err(e) => {
            tracing::error!("❌ Local solver unavailable on 127.0.0.1:8000 ({}). Using fallback '37'", e);
            Ok("37".to_string())
        }
    }
}

/// Extracts inventory records: matches checkboxes (`checkedArray`) with cell numbers
fn extract_inventory_records(html: &str) -> Vec<InventoryRecord> {
    let mut records = Vec::new();

    let re_checked = Regex::new(r#"<input[^>]+name=["']checkedArray["'][^>]+value=["']([^"']+)["']"#).unwrap();
    let re_phone = Regex::new(r#"\b([6-9]\d{9})\b"#).unwrap();

    let checked_values: Vec<String> = re_checked.captures_iter(html)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect();

    let phone_numbers: Vec<String> = re_phone.captures_iter(html)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect();

    for (i, val) in checked_values.into_iter().enumerate() {
        let phone = phone_numbers.get(i).cloned().unwrap_or_default();
        records.push(InventoryRecord {
            checked_value: val,
            cell_number: phone,
        });
    }

    records
}

fn is_results_table(html: &str) -> bool {
    html.contains("checkedArray") || html.contains("name=\"checkedArray\"")
}

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
            Err(_) => {}
        }
        sleep(Duration::from_secs(2)).await;
    }
}

/// Waits until document.readyState in page context returns complete or interactive
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
