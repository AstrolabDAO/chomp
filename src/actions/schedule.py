import asyncio

from src.cache import claim_task, is_task_claimed
import src.state as state
from src.utils import log_debug, log_error, log_warn
from src.model import Collector, CollectorType
import src.collectors as collectors

# mapping of available collectors to their respective functions
SCHEDULER_BY_TYPE: dict[CollectorType, callable] = {
  "scrapper": collectors.scrapper.schedule,
  "http_api": collectors.http_api.schedule,
  "ws_api": collectors.ws_api.schedule,
  # "fix_api": collectors.fix_api.schedule,
  # "evm": collectors.evm.schedule,
}

async def schedule(c: Collector) -> list[asyncio.Task]:
  if is_task_claimed(c):
    log_warn(f"Skipping collection for {c.name}-{c.interval}, task is already claimed by another worker...")
    return
  fn = SCHEDULER_BY_TYPE.get(c.collector_type, None)
  if not fn:
    raise ValueError(f"Unsupported collector type: {c.type}")
  claim_task(c)
  tasks = await fn(c)
  log_debug(f"Scheduled for collection: {c.name}-{c.interval} [{', '.join([field.name for field in c.data])}]")
  return tasks
