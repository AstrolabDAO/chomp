mod messages;
mod websocket_server;

use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    websocket_server::run_ws_forwarder().await;
}
