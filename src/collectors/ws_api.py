from collections import deque
from hashlib import md5
import asyncio
import json
import websockets

from src.model import Collector, ResourceField, Tsdb
from src.utils import floor_utc, log_debug, log_error, log_warn, select_from_dict
from src.actions.store import store
from src.actions.transform import transform
from src.cache import claim_task, ensure_claim_task
from src.safe_eval import safe_eval
import src.state as state

async def schedule(c: Collector) -> list[asyncio.Task]:
  epochs_by_route: dict[str, deque[dict]] = {}
  default_handler_by_route: dict[str, callable] = {}
  batched_fields_by_route: dict[str, list[ResourceField]] = {}

  # sub function (one per route)
  async def subscribe(c: Collector, url: str, route_hash: str):
    if state.verbose:
      log_debug(f"Subscribing to {url} for {c.name}.{c.interval}...")

    retry_count = 0

    while retry_count <= state.max_retries:
      try:
        async with websockets.connect(url) as ws:
          # initialize route state for reducers and transformers to use
          epochs_by_route.setdefault(url, deque([{}]))
          while True:
            if ws.closed:
              log_error(f"{url} ws connection closed for {c.name}")
              break
            res = await ws.recv() # poll for data
            res = json.loads(res)
            handled = {}
            for field in batched_fields_by_route[route_hash]:
              if field.handler and not handled.get(field.handler, {}).get(field.selector, False):
                data = select_from_dict(field.selector, res)
                try:
                  field.handler(data, epochs_by_route[url]) # map data with handler
                except Exception as e:
                  log_error(f"Failed to handle websocket data from {url} for {c.name}: {e}")
                handled.setdefault(field.handler, {})[field.selector] = True

            # if state.verbose: # <-- way too verbose
            #   log_debug(f"Handled websocket data {data} from {url} for {c.name}, route state:\n{epochs_by_route[url][0]}")

        # If we exit the loop without an exception, reset retry count
        retry_count = 0

      except (websockets.exceptions.ConnectionClosedError, ConnectionResetError) as e:
        retry_count += 1
        log_error(f"Connection error ({e}) occurred. Attempting to reconnect to {url} for {c.name} (retry {retry_count}/{state.max_retries})...")
        if retry_count > state.max_retries:
          log_error(f"Exceeded max retries ({state.max_retries}). Giving up on {url} for {c.name}.")
          break
        sleep_time = state.retry_cooldown * retry_count
        await asyncio.sleep(sleep_time)
      except Exception as e:
        log_error(f"Unexpected error occurred: {e}")
        retry_count += 1
        if retry_count > state.max_retries:
          log_error(f"Exceeded max retries ({state.max_retries}). Giving up on {url} for {c.name}.")
          break
        sleep_time = state.retry_cooldown * retry_count
        await asyncio.sleep(sleep_time)

  # collect function (one per collector)
  async def collect(c: Collector):
    if state.verbose:
      log_debug(f"Collecting {c.name}.{c.interval}")
    ensure_claim_task(c)
    # batch of reducers/transformers by route
    # iterate over key/value pairs
    for route_hash, batch in batched_fields_by_route.items():
      url = batch[0].target
      epochs = epochs_by_route.get(url, None)
      if not epochs[0]:
        log_warn(f"Missing state for {c.name} {url} collection, skipping...")
        continue
      for field in batch:
        # reduce the state to a collectable value
        field.value = field.reducer(epochs)
        if len(epochs) > 32: # keep the last 32 epochs (can be costly if many agg trades are stored in memory)
          epochs.pop()
        if state.verbose:
          log_debug(f"Reduced {c.name}.{field.name} -> {field.value}")
        # apply transformers to the field value if any
        if field.value and field.transformers:
          field.value = transform(c, field)
        if state.verbose:
          log_debug(f"Transformed {c.name}.{field.name} -> {field.value}")
        c.data_by_field[field.name] = field.value
      if state.verbose:
        log_debug(f"Appending epoch {len(epochs)} to {c.name}...")
      epochs.appendleft({}) # new epoch
    if state.verbose:
      log_debug(f"{c.name} collector state:\n{c.data_by_field}")
    c.collection_time = floor_utc(c.interval) # round down to theoretical task time
    await store(c)

  tasks = []
  for field in c.data:
    url = field.target

    # Create a unique key using a hash of the URL and interval
    route_hash = md5(f"{url}:{c.interval}".encode()).hexdigest()
    if url:
      # make sure that a field handler is defined if a target url is set
      if field.selector and not field.handler:
        if not route_hash in default_handler_by_route:
          raise ValueError(f"Missing handler for field {c.name}.{field.name} (selector {field.selector})")
        log_warn(f"Using {field.target} default field handler for {c.name}...")
        field.handler = default_handler_by_route[route_hash]
        batched_fields_by_route[route_hash].append(field)
        continue
      if isinstance(field.handler, str):
        field.handler = safe_eval(field.handler, callable_check=True) # compile the handler
      if isinstance(field.reducer, str):
        field.reducer = safe_eval(field.reducer, callable_check=True) # compile the reducer
      # batch the fields by route given we only need to subscribe once per route
      batched_fields_by_route.setdefault(route_hash, []).append(field)
      if not route_hash in default_handler_by_route and field.handler:
        default_handler_by_route[route_hash] = field.handler
      tasks.append(subscribe(c, url, route_hash))

  # globally register/schedule the collector
  return tasks + [state.add_cron(c.id, fn=collect, args=(c,), interval=c.interval)]
