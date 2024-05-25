import asyncio
from hashlib import md5
import json
import requests

from src.model import Collector
from src.utils import floor_utc, interval_to_delta, interval_to_seconds, log_debug, log_error, select_from_dict
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
      log_debug(f"Collecting {c.name}.{c.interval}")
    await ensure_claim_task(c)
    expiry_sec = interval_to_seconds(c.interval)
    for field in c.fields:
      url = field.target
      if not url:
        log_error(f"Missing target URL for field scrapper {c.name}.{field.name}, skipping...")
        continue
      url = url.strip().format(**c.data_by_field) # inject fields inside url if required

      # Create a unique key using a hash of the URL and interval
      route_hash = md5(f"{url}:{c.interval}".encode()).hexdigest()
      if not route_hash in data_by_route:
        data_str = await get_or_set_cache(route_hash, lambda: fetch_json(url), expiry_sec)
        try:
          data_by_route[route_hash] = json.loads(data_str)
        except Exception as e:
          log_error(f"Failed to parse JSON response from {url}: {e}")
          continue

      field.value = select_from_dict(field.selector, data_by_route[route_hash])

      # Apply transformations if any
      if field.value and field.transformers:
        field.value = transform(c, field)
      c.data_by_field[field.name] = field.value

    # reset local parser cache
    data_by_route.clear()
    c.collection_time = floor_utc(c.interval) # round down to theoretical task time
    await store(c)

  # globally register/schedule the collector
  return [await state.scheduler.add_collector(c, fn=collect, start=False)]

