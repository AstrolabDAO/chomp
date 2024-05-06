from hashlib import md5
import json
from actions.store import store
from actions.transform import transform
from cache import get_or_set_cache
import requests
from dateutil.relativedelta import relativedelta
from src.model import Collector, ResourceField, Tsdb
from src.utils import interval_to_delta, interval_to_seconds, log_error
from src.state import get_tsdb

def fetch_json(url: str) -> str:
  response = requests.get(url)
  if response.status_code == 200:
    return response.text
  return ""

def collect(ctx, c: Collector):
  data_by_route: dict[str, dict] = {}

  expiry_sec = interval_to_seconds(c.interval)
  for field in c.data:
    url = field.target
    if not url:
      log_error(f"Missing target URL for field scrapper {c.name}{field.name}, skipping...")
      continue

    # Create a unique key using a hash of the URL and interval
    route_hash = md5(f"{url}:{c.interval}".encode()).hexdigest()
    if not route_hash in data_by_route:
      data_str = get_or_set_cache(route_hash, lambda: fetch_json(url), expiry_sec)
      data_by_route[route_hash] = json.loads(data_str)

    data = data_by_route[route_hash]

    # Traverse the json with the selector
    if field.selector:
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
  store(c)
