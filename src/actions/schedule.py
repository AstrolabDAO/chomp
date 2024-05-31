import asyncio

from src.cache import claim_task, ensure_claim_task, is_task_claimed
import src.state as state
from src.utils import log_debug, log_error, log_warn
from src.model import Collector, CollectorType
import src.collectors as collectors

# mapping of available collectors to their respective functions
SCHEDULER_BY_TYPE: dict[CollectorType, callable] = {
  "scrapper": collectors.scrapper.schedule,
  "http_api": collectors.http_api.schedule,
  "ws_api": collectors.ws_api.schedule,
  "evm_caller": collectors.evm_caller.schedule,
  "evm_logger": collectors.evm_logger.schedule,
  # "fix_api": collectors.fix_api.schedule,
}

async def schedule(c: Collector) -> list[asyncio.Task]:
  fn = SCHEDULER_BY_TYPE.get(c.collector_type, None)
  if not fn:
    raise ValueError(f"Unsupported collector type: {c.type}")
  await ensure_claim_task(c)
  tasks = await fn(c)
  log_debug(f"Scheduled for collection: {c.name}.{c.interval} [{', '.join([field.name for field in c.fields])}]")
  return tasks
