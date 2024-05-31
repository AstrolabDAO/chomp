from datetime import datetime, UTC
from typing import Optional
import json

from src.utils import floor_utc, log_debug
import src.state as state
from src.model import Collector
from src.cache import cache, pub
from src.actions.transform import transform_all

async def store(c: Collector, table="", publish=True) -> list:
  data = json.dumps(c.values_dict())
  await cache(c.name, data) # max expiry
  if publish:
    await pub(c.name, data)
  if c.resource_type != "value":
    return await state.tsdb.insert(c, table)
  if state.args.verbose:
    log_debug(f"Collected and stored {c.name}-{c.interval}")

async def store_batch(c: Collector, values: list, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> dict:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    raise ValueError("Cannot store batch for inplace value collectors (series data required)")
  ok = await state.tsdb.insert_many(c, values, from_date, to_date, aggregation_interval)
  if state.args.verbose:
    log_debug(f"Collected and stored {len(values)} values for {c.name}-{c.interval} [{from_date} -> {to_date}]")
  return ok

async def transform_and_store(c: Collector, table="", publish=True):
  if transform_all(c) > 0:
    c.collection_time = floor_utc(c.interval)
    await store(c, table, publish)
  else:
    log_debug(f"No new values for {c.name}")
