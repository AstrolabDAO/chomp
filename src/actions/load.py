from datetime import datetime, UTC
from typing import Optional
import numpy as np

from src.model import Collector
from src.cache import cache, get_cache
import src.state as state

async def load(c: Collector, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> Collector|list|np.ndarray:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    return load_one(c)
  if not aggregation_interval:
    aggregation_interval = c.interval
  return await state.tsdb.fetch(c.name, from_date, to_date, aggregation_interval)

async def load_one(c: Collector) -> Collector|list|np.ndarray:
  return c.load_values(await get_cache(c.id))
