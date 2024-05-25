use std::env;
use warp::Filter;
use warp::ws::Message;
use warp::ws::Ws;
use tokio::sync::{broadcast, RwLock};
use futures::{StreamExt, SinkExt};
use std::collections::HashMap;
use std::sync::Arc;
use crate::messages::{WsRequest, WsResponse, Client};
use std::time::{Duration, Instant};

type Clients = Arc<RwLock<HashMap<String, Client>>>;
type Subscriptions = Arc<RwLock<HashMap<String, Vec<String>>>>;

pub async fn run_ws_forwarder() {
  let clients: Clients = Arc::new(RwLock::new(HashMap::new()));
  let subscriptions: Subscriptions = Arc::new(RwLock::new(HashMap::new()));
  let (tx, _) = broadcast::channel(100);

  let routes = warp::path("ws")
    .and(warp::ws())
    .and(warp::addr::remote())
    .and(with_clients(clients.clone()))
    .and(with_subscriptions(subscriptions.clone()))
    .and(with_broadcast_tx(tx.clone()))
    .map(|ws: Ws, addr: Option<std::net::SocketAddr>, clients, subscriptions, tx| {
      ws.on_upgrade(move |websocket| handle_connection(websocket, addr, clients, subscriptions, tx))
    });

  warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = std::convert::Infallible> + Clone {
  warp::any().map(move || clients.clone())
}

fn with_subscriptions(subscriptions: Subscriptions) -> impl Filter<Extract = (Subscriptions,), Error = std::convert::Infallible> + Clone {
  warp::any().map(move || subscriptions.clone())
}

fn with_broadcast_tx(tx: broadcast::Sender<String>) -> impl Filter<Extract = (broadcast::Sender<String>,), Error = std::convert::Infallible> + Clone {
  warp::any().map(move || tx.clone())
}

async fn handle_connection(ws: warp::ws::WebSocket, addr: Option<std::net::SocketAddr>, clients: Clients, subscriptions: Subscriptions, tx: broadcast::Sender<String>) {
  let (mut ws_tx, mut ws_rx) = ws.split();
  let ip = addr.map(|a| a.ip().to_string()).unwrap_or_default();

  if count_connections(&clients, &ip).await >= 10 {
    return;
  }

  let client_id = uuid::Uuid::new_v4().to_string();
  let mut client = Client { id: client_id.clone(), ip: ip.clone(), subscriptions: HashSet::new() };

  {
    let mut clients_lock = clients.write().await;
    clients_lock.insert(client_id.clone(), client.clone());
  }

  tokio::spawn(subscribe_redis(tx.subscribe(), ws_tx.clone()));

  while let Some(result) = ws_rx.next().await {
    match result {
      Ok(msg) => if let Ok(text) = msg.to_str() {
        handle_message(text, &mut client, &clients, &subscriptions, &mut ws_tx, &tx).await;
      },
      Err(e) => eprintln!("WebSocket error: {}", e),
    }

    if !client_alive(&client_id, &clients).await {
      break;
    }
  }

  {
    let mut clients_lock = clients.write().await;
    clients_lock.remove(&client_id);
  }

  unsubscribe_all(&client, &subscriptions).await;
}

async fn handle_message(text: &str, client: &mut Client, clients: &Clients, subscriptions: &Subscriptions, ws_tx: &mut warp::ws::WebSocket, tx: &broadcast::Sender<String>) {
  if let Ok(request) = serde_json::from_str::<WsRequest>(text) {
    match request.method.as_str() {
      "sub" => if let Some(resources) = request.resources {
        for resource in resources {
          client.subscriptions.insert(resource.clone());
          add_subscription(&client.id, &resource, subscriptions).await;
        }
        send_response(ws_tx, "ok", "Subscribed successfully").await;
      },
      "unsub" => if let Some(resources) = request.resources {
        for resource in resources {
          client.subscriptions.remove(&resource);
          remove_subscription(&client.id, &resource, subscriptions).await;
        }
        send_response(ws_tx, "ok", "Unsubscribed successfully").await;
      },
      "keepalive" => send_response(ws_tx, "ok", "Keepalive received").await,
      _ => send_response(ws_tx, "error", "Unknown method").await,
    }
  }
}

async fn send_response(ws_tx: &mut warp::ws::WebSocket, status: &str, message: &str) {
  let response = WsResponse { status: status.to_string(), message: message.to_string() };
  ws_tx.send(Message::text(serde_json::to_string(&response).unwrap())).await.unwrap();
}

async fn count_connections(clients: &Clients, ip: &str) -> usize {
  clients.read().await.values().filter(|client| client.ip == ip).count()
}

async fn client_alive(client_id: &str, clients: &Clients) -> bool {
  clients.read().await.contains_key(client_id)
}

async fn add_subscription(client_id: &str, resource: &str, subscriptions: &Subscriptions) {
  let mut subscriptions_lock = subscriptions.write().await;
  subscriptions_lock.entry(resource.to_string()).or_insert_with(Vec::new).push(client_id.to_string());
}

async fn remove_subscription(client_id: &str, resource: &str, subscriptions: &Subscriptions) {
  let mut subscriptions_lock = subscriptions.write().await;
  if let Some(subs) = subscriptions_lock.get_mut(resource) {
    subs.retain(|id| id != client_id);
    if subs.is_empty() {
      subscriptions_lock.remove(resource);
    }
  }
}

async fn unsubscribe_all(client: &Client, subscriptions: &Subscriptions) {
  let mut subscriptions_lock = subscriptions.write().await;
  for resource in &client.subscriptions {
    if let Some(subs) = subscriptions_lock.get_mut(resource) {
      subs.retain(|id| id != &client.id);
      if subs.is_empty() {
        subscriptions_lock.remove(resource);
      }
    }
  }
}

async fn subscribe_redis(mut rx: broadcast::Receiver<String>, mut ws_tx: warp::ws::WebSocket) {
  while let Ok(msg) = rx.recv().await {
    ws_tx.send(Message::text(msg)).await.unwrap();
  }
}
