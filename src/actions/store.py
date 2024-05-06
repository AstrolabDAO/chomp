from datetime import datetime
import json

from src.state import get_tsdb
from src.model import CollectorType, Resource, Collector, Interval, ResourceType, Tsdb
from src.cache import cache

def store(c: Collector, table="") -> list:
  if c.resource_type == ResourceType.VALUE:
    return cache(c.id, json.dumps(c.values_dict())) # max expiry
  return get_tsdb().insert(c, table)

def store_batch(db: Tsdb, c: Collector, values: list, from_date: datetime, to_date: datetime=datetime.now(), aggregation_interval=None) -> dict:
  if c.resource_type == ResourceType.VALUE:
    raise ValueError("Cannot store batch for inplace value collectors (series data required)")
  return db.insert_many(c, values, from_date, to_date, aggregation_interval)
