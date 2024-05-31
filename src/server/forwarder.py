import aioredis
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, WebSocketState
from fastapi.responses import HTMLResponse
from typing import Dict, Set
from datetime import datetime, timedelta
import json

app = FastAPI()

# Redis connection and pub/sub setup
redis_client = None
pubsub = None

# Data structures for client management
connected_clients: Dict[WebSocket, Set[str]] = {}
keepalive_timestamps: Dict[WebSocket, datetime] = {}

@app.on_event("startup")
async def startup():
  global redis_client, pubsub
  redis_client = await aioredis.create_redis_pool('redis://localhost')
  pubsub = redis_client.pubsub()
  asyncio.create_task(handle_redis_messages())
  asyncio.create_task(check_keepalive())

async def handle_redis_messages():
  while True:
    try:
      message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
      if message:
        topic = message['channel'].decode('utf-8')
        data = message['data'].decode('utf-8')
        await broadcast_message(topic, data)
    except Exception as e:
      print(f"Error handling Redis messages: {e}")

async def broadcast_message(topic, message):
  for client, subscriptions in connected_clients.items():
    if topic in subscriptions and client.client_state == WebSocketState.CONNECTED:
      try:
        await client.send_text(message)
      except Exception as e:
        print(f"Error sending message to client: {e}")

async def check_keepalive():
  while True:
    await asyncio.sleep(60)
    now = datetime.now()
    for client, timestamp in list(keepalive_timestamps.items()):
      if now - timestamp > timedelta(minutes=30):
        await client.close()
        await remove_client(client)

async def remove_client(client):
  for topic in connected_clients[client]:
    await pubsub.unsubscribe(topic)
  del connected_clients[client]
  del keepalive_timestamps[client]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
  await websocket.accept()
  connected_clients[websocket] = set()
  keepalive_timestamps[websocket] = datetime.now()

  try:
    while True:
      data = await websocket.receive_text()
      message = json.loads(data)

      if message["type"] == "subscribe":
        topics = message["topics"]
        for topic in topics:
          connected_clients[websocket].add(topic)
          await pubsub.subscribe(topic)

      elif message["type"] == "unsubscribe":
        topics = message["topics"]
        for topic in topics:
          connected_clients[websocket].discard(topic)
          await pubsub.unsubscribe(topic)

      elif message["type"] == "keepalive":
        keepalive_timestamps[websocket] = datetime.now()

      else:
        # Handle invalid message types
        pass

  except WebSocketDisconnect:
    await remove_client(websocket)

@app.get("/")
async def get():
  html_content = """
  <!DOCTYPE html>
  <html>
  <head>
    <title>WebSocket Test</title>
  </head>
  <body>
    <h1>WebSocket Test</h1>
  </body>
  </html>
  """
  return HTMLResponse(html_content)
