from datetime import UTC, datetime
from os import environ as env

from src.model import Collector, Interval
import src.state as state
from src.state import get_redis
from src.utils import YEAR_SECONDS, interval_to_delta, log_debug, log_error, log_warn

NS = env.get("REDIS_NS", "collector")

# clustering/synchronization
def claim_key(c: Collector) -> str:
  return f"{NS}:{c.id}:claimed"

def claim_task(c: Collector, until=0, key="") -> bool:
  if state.verbose:
    log_debug(f"Claiming task {c.name}-{c.interval}")
  # if c.collection_time and c.collection_time > (datetime.now(UTC) - interval_to_delta(c.interval)):
  #   log_warn(f"Collection time inconsistent for {c.name}-{c.interval}, last collection was {c.collection_time}, probably due to a slow running collection or local worker race... investigate!")
  key = key or claim_key(c)
  if is_task_claimed(c, True, key):
    return False
  return get_redis().setex(key, round(until or c.interval_sec * 1.2), state.proc_id) # 20% overtime buffer for long running tasks

def ensure_claim_task(c: Collector, until=0) -> bool:
  if not claim_task(c, until):
    raise ValueError(f"Failed to claim task {c.name}-{c.interval})")

def is_task_claimed(c: Collector, exclude_self=False, key="") -> bool:
  key = key or claim_key(c)
  val = get_redis().get(key)
  # unclaimed and uncontested
  return bool(val) and (not exclude_self or val.decode() != state.proc_id)

def free_task(c: Collector, key="") -> bool:
  key = key or claim_key(c)
  if not is_task_claimed(key) or get_redis().get(key) != state.proc_id:
    return False
  return get_redis().delete(key)

# caching
def cache_key(name: str) -> str:
  return f"{NS}:{name}"

def cache(name: str, value: str|int|float|bool, expiry: int=YEAR_SECONDS) -> bool:
  return get_redis().setex(cache_key(name), round(expiry), value)

def get_cache(name: str):
  return get_redis().get(cache_key(name))

def get_or_set_cache(name: str, callback: callable, expiry: int):
  key = cache_key(name)
  value = get_cache(key)
  if not value:
    value = callback() # TODO: add support for awaitable callbacks
    if value == None or value == "":
      log_warn(f"Cache could not be rehydrated for key: {key}")
      return None
    cache(key, value, expiry)
  return value
