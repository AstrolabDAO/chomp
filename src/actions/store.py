from datetime import datetime, UTC
import json

from src.state import get_tsdb
from src.model import CollectorType, Resource, Collector, Interval, ResourceType, Tsdb
from src.cache import cache

async def store(c: Collector, table="") -> list:
  if c.resource_type == "value":
    return cache(c.id, json.dumps(c.values_dict())) # max expiry
  return await (get_tsdb().insert(c, table))

async def store_batch(c: Collector, values: list, from_date: datetime, to_date: datetime=datetime.now(UTC), aggregation_interval=None) -> dict:
  if c.resource_type == "value":
    raise ValueError("Cannot store batch for inplace value collectors (series data required)")
  return await (get_tsdb().insert_many(c, values, from_date, to_date, aggregation_interval))
