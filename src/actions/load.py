from datetime import datetime
import numpy as np
from src.state import get_tsdb
from src.cache import cache, get_cache
from src.model import Collector, Resource, Interval, ResourceType, Tsdb


def load(c: Collector, from_date: datetime, to_date: datetime=datetime.now(), aggregation_interval=None) -> Collector|list|np.ndarray:
  if c.resource_type == ResourceType.VALUE:
    return load_one(c)
  if not aggregation_interval:
    aggregation_interval = c.interval
  return load_series(get_tsdb(), c, from_date, to_date, aggregation_interval)

def load_one(c: Collector) -> Collector|list|np.ndarray:
  return c.load_values(get_cache(c.id))

def load_series(c: Collector, from_date: datetime, to_date: datetime=datetime.now(), aggregation_interval=None) -> dict:
  return get_tsdb().fetch_series(c.name, from_date, to_date, aggregation_interval)
