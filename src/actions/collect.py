from datetime import datetime, UTC
import src.state as state
from src.utils import floor_utc, interval_to_delta, log_debug, log_error, log_warn
from src.model import Collector, CollectorType
from src.collectors.scrapper import collect as scrape_collect
from src.collectors.api import collect as fetch_collect
from src.collectors.evm import collect as evm_call_collect

# mapping of available collectors to their respective functions
COLLECTOR_BY_TYPE: dict[CollectorType, callable] = {
  CollectorType.SCRAPPER: scrape_collect,
  CollectorType.API: fetch_collect,
  CollectorType.EVM: evm_call_collect
}

def collect(ctx, c: Collector):
  if c.collection_time and c.collection_time > (datetime.now(UTC) - interval_to_delta(c.interval)):
    log_warn(f"Skipping collection for {c.name}-{c.interval}, last collection was {c.collection_time}, probably due to a worker race... investigate!")
    return
  c.collection_time = floor_utc(c.interval)
  if state.verbose:
    log_debug(f"Collecting {c.name}-{c.interval}: [{', '.join([field.name for field in c.data])}]")
  fn = COLLECTOR_BY_TYPE.get(c.collector_type, None)
  if not fn:
    raise ValueError(f"Unsupported collector type: {c.type}")
  fn(ctx, c)
  if state.verbose:
    log_debug(f"Collected {c.name}-{c.interval}:\n{c.values_dict()}")
