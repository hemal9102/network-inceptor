# network-inceptor

[![Rust](https://img.shields.io/badge/language-Rust%202021-orange.svg)](https://www.rust-lang.org/)
[![Runtime](https://img.shields.io/badge/runtime-Tokio%20Async-blue.svg)](https://tokio.rs/)
[![Protocol](https://img.shields.io/badge/protocol-CDP%20WebSocket-purple.svg)](https://chromedevtools.github.io/devtools-protocol/)

High-performance, asynchronous network telemetry and traffic interception engine written in Rust. Designed for sub-millisecond packet capture, live Chrome DevTools Protocol (CDP) WebSocket event streaming, and automated request-response lifecycle mutation.

---

## Architecture & Systems Design


etwork-inceptor establishes an asynchronous full-duplex WebSocket connection to the browser runtime via Chrome DevTools Protocol (CDP). It inspects, filters, and processes network frames at wire speed using zero-copy stream decoders.

`
                      +---------------------------------------+
                      |       Headless Browser Runtime        |
                      |   (Edge / Chrome with Remote CDP)     |
                      +-------------------+-------------------+
                                          |
                              WebSocket (ws://127.0.0.1:9222)
                                          |
                                          v
                      +-------------------+-------------------+
                      |      network-inceptor (Rust Engine)   |
                      |                                       |
                      |  * tokio multi-threaded runtime       |
                      |  * tokio-tungstenite async stream     |
                      |  * AtomicI64 command sequencing       |
                      |  * Zero-copy JSON & payload decoding  |
                      |  * Regex pattern stream matching      |
                      +-------------------+-------------------+
                                          |
                                          v
                      +-------------------+-------------------+
                      |   Extracted Telemetry & Telemetry Sink|
                      +---------------------------------------+
`

---

## Key Primitives

- **Asynchronous WebSocket Transport**: Powered by 	okio-tungstenite and utures-util for non-blocking frame handling over persistent socket connections.
- **Thread-Safe State & Atomic Sequencing**: Employs std::sync::atomic::AtomicI64 with relaxed/acquire-release ordering to guarantee monotonic message ID sequencing across concurrent async tasks without lock contention.
- **Dynamic CDP Domain Control**: Enables Network, Page, and Runtime domains dynamically to subscribe to low-level network lifecycle events (Network.requestWillBeSent, Network.responseReceived, Network.loadingFinished).
- **High-Throughput Regex & Base64 Stream Decoding**: Decodes binary and compressed payloads on-the-fly with minimal heap allocations.

---

## Technical Stack

| Component | Crate / Technology | Purpose |
| :--- | :--- | :--- |
| **Async Runtime** | 	okio 1.x (ull) | Multi-threaded task scheduling, timers, and async I/O. |
| **WebSocket** | 	okio-tungstenite 0.24 | Raw asynchronous WebSocket client for CDP wire telemetry. |
| **Serialization** | serde_json 1.0 | High-efficiency JSON parsing and payload extraction. |
| **Regex Matching**| 
egex 1.x | Compiled regular expressions for deep payload inspection. |
| **HTTP Client** | 
eqwest 0.12 | Initial target discovery and browser session bootstrap. |
| **Logging** | 	racing & 	racing-subscriber | Structured, zero-cost operational observability. |

---

## Build & Run

### Prerequisites
- Rust 1.75+ (Cargo toolchain)
- Chrome or Edge instance launched with --remote-debugging-port=9222

`ash
# Launch Edge with remote CDP port
launch_edge.bat

# Build optimized binary
cargo build --release

# Run the interception engine
cargo run --release
`

---

## Author
**Hemal Shah**  
Systems & AI Platform Engineer  
[https://tensix.in](https://tensix.in) | [github.com/hemal9102](https://github.com/hemal9102)
