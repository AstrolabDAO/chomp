from datetime import datetime, UTC
import pickle
from typing import Optional

from src.utils.date import floor_utc
from src.utils.format import log_debug
import src.state as state
from src.model import Ingester
from src.cache import cache, pub
from src.actions.transform import transform_all

async def store(c: Ingester, table="", publish=True) -> list:
  data = pickle.dumps(c.values_dict())
  await cache(c.name, data) # max expiry
  if publish:
    await pub(c.name, data)
  if c.resource_type != "value":
    return await state.tsdb.insert(c, table)
  if state.args.verbose:
    log_debug(f"Ingested and stored {c.name}-{c.interval}")

async def store_batch(c: Ingester, values: list, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> dict:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    raise ValueError("Cannot store batch for inplace value ingesters (series data required)")
  ok = await state.tsdb.insert_many(c, values, from_date, to_date, aggregation_interval)
  if state.args.verbose:
    log_debug(f"Ingested and stored {len(values)} values for {c.name}-{c.interval} [{from_date} -> {to_date}]")
  return ok

async def transform_and_store(c: Ingester, table="", publish=True):
  if transform_all(c) > 0:
    c.ingestion_time = floor_utc(c.interval)
    await store(c, table, publish)
  else:
    log_debug(f"No new values for {c.name}")
