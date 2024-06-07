from collections import deque
from hashlib import md5
from asyncio import gather, Task, sleep
import json
import websockets

from src.model import Ingester, ResourceField, Tsdb
from src.utils import log_debug, log_error, log_warn, select_nested, floor_utc, safe_eval
from src.actions import store, transform, scheduler
from src.cache import claim_task, ensure_claim_task
import src.state as state

async def schedule(c: Ingester) -> list[Task]:

  epochs_by_route: dict[str, deque[dict]] = {}
  default_handler_by_route: dict[str, callable] = {}
  batched_fields_by_route: dict[str, list[ResourceField]] = {}
  subscriptions = set()

  # sub function (one per route)
  async def subscribe(c: Ingester, field: ResourceField, route_hash: str):

    url = field.target
    if state.args.verbose:
      log_debug(f"Subscribing to {url} for {c.name}.{field.name}.{c.interval}...")

    retry_count = 0

    while retry_count <= state.args.max_retries:
      try:
        async with websockets.connect(url) as ws:
          if field.params:
            await ws.send(json.dumps(field.params)) # send subscription params if any (eg. api key, stream list...)
          # initialize route state for reducers and transformers to use
          epochs_by_route.setdefault(url, deque([{}]))
          while True:
            if ws.closed:
              log_error(f"{url} ws connection closed for {c.name}.{field.name}...")
              break
            res = await ws.recv() # poll for data
            res = json.loads(res)
            handled = {}
            for field in batched_fields_by_route[route_hash]:
              if field.handler and not handled.setdefault(field.handler, {}).get(field.selector, False):
                try:
                  data = select_nested(field.selector, res)
                  if data:
                    field.handler(data, epochs_by_route[url]) # map data with handler
                    pass
                except Exception as e:
                  log_warn(f"Failed to handle websocket data from {url} for {c.name}.{field.name}: {e}")
                handled.setdefault(field.handler, {})[field.selector] = True

            # if state.args.verbose: # <-- way too verbose
            #   log_debug(f"Handled websocket data {data} from {url} for {c.name}, route state:\n{epochs_by_route[url][0]}")

        # If we exit the loop without an exception, reset retry count
        retry_count = 0

      except (websockets.exceptions.ConnectionClosedError, ConnectionResetError) as e:
        retry_count += 1
        log_error(f"Connection error ({e}) occurred. Attempting to reconnect to {url} for {c.name} (retry {retry_count}/{state.args.max_retries})...")
        if retry_count > state.args.max_retries:
          log_error(f"Exceeded max retries ({state.args.max_retries}). Giving up on {url} for {c.name}.")
          break
        sleep_time = state.args.retry_cooldown * retry_count
        await sleep(sleep_time)
      except Exception as e:
        log_error(f"Unexpected error occurred: {e}")
        retry_count += 1
        if retry_count > state.args.max_retries:
          log_error(f"Exceeded max retries ({state.args.max_retries}). Giving up on {url} for {c.name}.")
          break
        sleep_time = state.args.retry_cooldown * retry_count
        await sleep(sleep_time)

  # collect function (one per ingester)
  async def ingest(c: Ingester):
    await ensure_claim_task(c)
    # batch of reducers/transformers by route
    # iterate over key/value pairs
    collected_batches = 0
    for route_hash, batch in batched_fields_by_route.items():
      url = batch[0].target
      epochs = epochs_by_route.get(url, None)
      if not epochs or not epochs[0]:
        log_warn(f"Missing state for {c.name} {url} ingestion, skipping...")
        continue
      collected_batches += 1
      for field in batch:
        # reduce the state to a collectable value
        try:
          field.value = field.reducer(epochs) if field.reducer else None
        except Exception as e:
          log_warn(f"Failed to reduce {c.name}.{field.name} for {url}, epoch attributes maye be missing: {e}")
          continue
        if len(epochs) > 32: # keep the last 32 epochs (can be costly if many agg trades are stored in memory)
          epochs.pop()
        if state.args.verbose:
          log_debug(f"Reduced {c.name}.{field.name} -> {field.value}")
        # apply transformers to the field value if any
        if field.transformers:
          field.value = transform(c, field)
        if state.args.verbose:
          log_debug(f"Transformed {c.name}.{field.name} -> {field.value}")
      if state.args.verbose:
        log_debug(f"Appending epoch {len(epochs)} to {c.name}...")
      epochs.appendleft({}) # new epoch
    if state.args.verbose:
      log_debug(f"{c.name} ingester state:\n{c.data_by_field}")
    if collected_batches > 0:
      c.ingestion_time = floor_utc(c.interval) # round down to theoretical task time
      await store(c)
    else:
      log_warn(f"No data collected for {c.name}, waiting for ws state to aggregate...")

  tasks = []
  for field in c.fields:
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
      if field.handler and isinstance(field.handler, str):
        field.handler = safe_eval(field.handler, callable_check=True) # compile the handler
      if field.reducer and isinstance(field.reducer, str):
        try:
          field.reducer = safe_eval(field.reducer, callable_check=True) # compile the reducer
        except Exception as e:
          continue
      # batch the fields by route given we only need to subscribe once per route
      batched_fields_by_route.setdefault(route_hash, []).append(field)
      if not route_hash in default_handler_by_route and field.handler:
        default_handler_by_route[route_hash] = field.handler
      if field.target_id in subscriptions:
        continue # only subscribe once per socket route+selector+params combo
      subscriptions.add(field.target_id)
      tasks.append(subscribe(c, field, route_hash))

  # subscribe all at once, run in the background
  gather(*tasks)

  # register/schedule the ingester
  return [await scheduler.add_ingester(c, fn=ingest, start=False)]
