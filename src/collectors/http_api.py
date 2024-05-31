from asyncio import Task, gather
from hashlib import md5
import json
from aiohttp import ClientSession

from src.model import Collector
from src.utils import log_error, select_nested
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state
from src.actions.store import transform_and_store

async def fetch_json(url: str) -> str:
  async with ClientSession() as session:
    async with session.get(url) as response:
      if response.status == 200:
        return await response.text()
      return ""

async def schedule(c: Collector) -> list[Task]:

  data_by_route: dict[str, dict] = {}
  hashes: dict[str, str] = {}

  async def collect(c: Collector):
    await ensure_claim_task(c)

    async def fetch_hashed(url: str) -> dict:
      data_str = await get_or_set_cache(hashes[url], lambda: fetch_json(url), c.interval_sec)
      try:
        data_by_route[hashes[url]] = json.loads(data_str)
      except Exception as e:
        log_error(f"Failed to parse JSON response from {url}: {e}")

    fetch_tasks = []
    for field in c.fields:
      if field.target:
        url = field.target
        url = url.strip().format(**c.data_by_field) # inject fields inside url if required

        # Create a unique key using a hash of the URL and interval
        if not url in hashes:
          hashes[url] = md5(f"{url}:{c.interval}".encode()).hexdigest()
        if not hashes[url] in data_by_route:
          fetch_tasks.append(fetch_hashed(url))

    await gather(*fetch_tasks)

    for field in [f for f in c.fields if f.target]:
      field.value = select_nested(field.selector, data_by_route[hashes[field.target]])

    await transform_and_store(c)

    # reset local parser cache
    data_by_route.clear()

  # globally register/schedule the collector
  return [await state.scheduler.add_collector(c, fn=collect, start=False)]
