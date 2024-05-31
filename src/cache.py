#TODO: batch redis tx commit whenever possible

from asyncio import gather, iscoroutinefunction, iscoroutine
from os import environ as env

from src.model import Collector, Interval
import src.state as state
from src.state import redis
from src.utils import YEAR_SECONDS, interval_to_delta, log_debug, log_error, log_warn

NS = env.get("REDIS_NS", "chomp")

# clustering/synchronization
def claim_key(c: Collector) -> str:
  return f"{NS}:claims:{c.id}"

async def claim_task(c: Collector, until=0, key="") -> bool:
  if state.args.verbose:
    log_debug(f"Claiming task {c.name}.{c.interval}")
  # if c.collection_time and c.collection_time > (datetime.now(UTC) - interval_to_delta(c.interval)):
  #   log_warn(f"Collection time inconsistent for {c.name}.{c.interval}, last collection was {c.collection_time}, probably due to a slow running collection or local worker race... investigate!")
  key = key or claim_key(c)
  if await is_task_claimed(c, True, key):
    return False
  return await redis.setex(key, round(until or c.interval_sec * 1.2), state.args.proc_id) # 20% overtime buffer for long running tasks

async def ensure_claim_task(c: Collector, until=0) -> bool:
  if not await claim_task(c, until):
    raise ValueError(f"Failed to claim task {c.name}.{c.interval})")

async def is_task_claimed(c: Collector, exclude_self=False, key="") -> bool:
  key = key or claim_key(c)
  val = await redis.get(key)
  # unclaimed and uncontested
  return bool(val) and (not exclude_self or val.decode() != state.args.proc_id)

async def free_task(c: Collector, key="") -> bool:
  key = key or claim_key(c)
  if not (await is_task_claimed(key)) or await redis.get(key) != state.args.proc_id:
    return False
  return await redis.delete(key)

# caching
def cache_key(name: str) -> str:
  return f"{NS}:cache:{name}"

async def cache(name: str, value: str|int|float|bool, expiry: int=YEAR_SECONDS) -> bool:
  return await redis.setex(cache_key(name), round(expiry), value)

async def get_cache(name: str):
  return await redis.get(cache_key(name))

async def get_or_set_cache(name: str, callback: callable, expiry: int):
  key = cache_key(name)
  value = await get_cache(key)
  if not value:
    value = callback() if not iscoroutinefunction(callback) else await callback()
    if iscoroutine(value):
      value = await value
    if value in (None, ""):
      log_warn(f"Cache could not be rehydrated for key: {key}")
      return None
    await cache(key, value, expiry)
  return value

async def pub(topics: list[str], msg: str):
  tasks = []
  for topic in topics:
    tasks.append(redis.publish(f"{NS}:{topic}", msg))
  return await gather(*tasks)

async def sub(topics: list[str], handler: callable):
  sub = redis.pubsub()
  await sub.subscribe(*topics)
  for msg in sub.listen():
    if msg["type"] == "message":
      handler(msg["data"])
