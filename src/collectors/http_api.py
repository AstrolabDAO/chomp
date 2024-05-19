import asyncio
from hashlib import md5
import json
import requests

from src.model import Collector
from src.utils import floor_utc, interval_to_delta, interval_to_seconds, log_debug, log_error
from src.actions.store import store
from src.actions.transform import transform
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state

def fetch_json(url: str) -> str:
  response = requests.get(url)
  if response.status_code == 200:
    return response.text
  return ""

async def schedule(c: Collector) -> list[asyncio.Task]:

  data_by_route: dict[str, dict] = {}

  async def collect(c: Collector):
    if state.verbose:
      log_debug(f"Collecting {c.name}-{c.interval}")
    ensure_claim_task(c)
    expiry_sec = interval_to_seconds(c.interval)
    for field in c.data:
      url = field.target
      if not url:
        log_error(f"Missing target URL for field scrapper {c.name}.{field.name}, skipping...")
        continue

      # Create a unique key using a hash of the URL and interval
      route_hash = md5(f"{url}:{c.interval}".encode()).hexdigest()
      if not route_hash in data_by_route:
        data_str = get_or_set_cache(route_hash, lambda: fetch_json(url), expiry_sec)
        data_by_route[route_hash] = json.loads(data_str)

      data = data_by_route[route_hash]

      # Traverse the json with the selector
      if field.selector not in {"", ".", "root"}:
        if field.selector.startswith("."):
          field.selector = field.selector[1:]
        for key in field.selector.split("."):
          data = data.get(key, None)
          if not data:
            log_error(f"Failed to find element {field.selector} in fetched json {url}, skipping...")
            break
      field.value = data

      # Apply transformations if any
      if field.value and field.transformers:
        field.value = transform(c, field)
      c.data_by_field[field.name] = field.value

    # reset local parser cache
    data_by_route.clear()
    c.collection_time = floor_utc(c.interval) # round down to theoretical task time
    await store(c)

  # globally register/schedule the collector
  return [state.add_cron(c.id, fn=collect, args=(c,), interval=c.interval)]
