import json
from asyncio import sleep, gather, Task
import uvicorn
from fastapi import APIRouter
from fastapi.websockets import WebSocketState, WebSocket, WebSocketDisconnect
from fastapi.concurrency import asynccontextmanager
from fastapi.responses import HTMLResponse
from typing import Literal
from src.utils import log_debug, log_error
import src.state as state

WsAction = Literal["subscribe", "unsubscribe", "ping", "keepalive"]

# Redis connection and pub/sub setup
redis_client = None
pubsub = None

# Data structures for client management
clients_by_topic: dict[str, set[WebSocket]] = {}
topics_by_client: dict[WebSocket, set[str]] = {}

topics = set()

@asynccontextmanager
async def lifespan(router: APIRouter):
  # on startup
  global pubsub
  pubsub = state.redis.pubsub()
  tasks = [
    handle_redis_messages(),
    # check_keepalive() # already handled by uvicorn
  ]

  # run app
  yield

  # on shutdown
  gather(*tasks)

router = APIRouter(lifespan=lifespan)

async def handle_redis_messages():
  while True:
    try:
      msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
      if msg:
        topic = msg['channel'].decode('utf-8')
        data = msg['data'].decode('utf-8')
        if topic in topics:
          await broadcast_message(topic, data)
    except Exception as e:
      log_error(f"Error handling Redis messages: {e}")

async def broadcast_message(topic, msg):
  for ws in clients_by_topic.get(topic, []):
    if ws.client_state == WebSocketState.DISCONNECTED:
      await disconnect_client(ws)
    try:
      await ws.send_text(msg)
    except Exception as e:
      log_error(f"Error sending message to client: {e}")

async def disconnect_client(ws: WebSocket):
  await ws.close()
  for topic in topics_by_client[ws]:
    await pubsub.unsubscribe(topic)
    clients_by_topic[topic].remove(ws)
  del topics_by_client[ws]
  if state.ars.verbose:
    log_debug(f"Disconnected client {ws}")

@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
  await ws.accept()
  topics_by_client[ws] = set()

  try:
    while True:
      msg = json.loads(await ws.receive_text())
      act: WsAction = msg.get("action")

      match act:
        case "subscribe":
          topics = msg["topics"]
          for topic in topics:
            if not await state.redis.exists():
              log_error(f"Requested topic {topic} does not exist, skipping...")
              # TODO: return 404 and stop hadshake
            topics_by_client[ws].add(topic)
            await pubsub.subscribe(topic)
        case "unsubscribe":
          topics = msg["topics"]
          for topic in topics:
            topics_by_client[ws].discard(topic)
            await pubsub.unsubscribe(topic)
        case _:
          # TODO: return 404 and stop hadshake
          log_error(f"Invalid ws action: {msg}, skipping...")

  except WebSocketDisconnect:
    await disconnect_client(ws)
