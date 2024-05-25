from datetime import datetime, UTC
from typing import Optional
import json

import src.state as state
from src.model import Collector
from src.cache import cache, pub

async def store(c: Collector, table="", cache=True, publish=True) -> list:
  data = json.dumps(c.values_dict())
  if cache:
    await cache(c.name, data) # max expiry
  if publish:
    await pub(c.name, data)
  if c.resource_type != "value":
    return await state.tsdb.insert(c, table)

async def store_batch(c: Collector, values: list, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> dict:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    raise ValueError("Cannot store batch for inplace value collectors (series data required)")
  return await state.tsdb.insert_many(c, values, from_date, to_date, aggregation_interval)
