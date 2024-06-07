# TODO: batch redis tx commit whenever possible (cf. limiter.py)
from asyncio import gather, iscoroutinefunction, iscoroutine
from os import environ as env
import pickle

from src.model import Ingester
import src.state as state
from src.state import redis
from src.utils import log_debug, log_error, log_warn, YEAR_SECONDS

NS = env.get("REDIS_NS", "chomp")

# clustering/synchronization
def claim_key(c: Ingester) -> str:
  return f"{NS}:claims:{c.id}"

async def claim_task(c: Ingester, until=0, key="") -> bool:
  if state.args.verbose:
    log_debug(f"Claiming task {c.name}.{c.interval}")
  # if c.ingestion_time and c.ingestion_time > (datetime.now(UTC) - interval_to_delta(c.interval)):
  #   log_warn(f"Ingestion time inconsistent for {c.name}.{c.interval}, last ingestion was {c.ingestion_time}, probably due to a slow running ingestion or local worker race... investigate!")
  key = key or claim_key(c)
  if await is_task_claimed(c, True, key):
    return False
  return await redis.setex(key, round(until or c.interval_sec * 1.2), state.args.proc_id) # 20% overtime buffer for long running tasks

async def ensure_claim_task(c: Ingester, until=0) -> bool:
  if not await claim_task(c, until):
    raise ValueError(f"Failed to claim task {c.name}.{c.interval})")

async def is_task_claimed(c: Ingester, exclude_self=False, key="") -> bool:
  key = key or claim_key(c)
  val = await redis.get(key)
  # unclaimed and uncontested
  return bool(val) and (not exclude_self or val.decode() != state.args.proc_id)

async def free_task(c: Ingester, key="") -> bool:
  key = key or claim_key(c)
  if not (await is_task_claimed(key)) or await redis.get(key) != state.args.proc_id:
    return False
  return await redis.delete(key)

# caching
def cache_key(name: str) -> str:
  return f"{NS}:cache:{name}"

async def cache(name: str, value: str|int|float|bool, expiry=YEAR_SECONDS, raw_key=False, encoding="", pickled=False) -> bool:
  return await redis.setex(name if raw_key else cache_key(name),
    round(expiry),
    pickle.dumps(value) if pickled else \
      value.encode(encoding) if encoding else \
        value)

async def cache_batch(data: dict, expiry=YEAR_SECONDS, pickled=False, encoding="", raw_key: bool = False) -> None:
  expiry = round(expiry)
  keys_values = {
    name if raw_key else cache_key(name): pickle.dumps(value) if pickled else value.encode(encoding) if encoding else value
    for name, value in data.items()
  }
  async with redis.pipeline() as pipe:
    for key, value in keys_values.items():
      pipe.psetex(key, expiry, value)
    await pipe.execute()

async def get_cache(name: str, pickled=False, encoding="", raw_key=False):
  r = await redis.get(name if raw_key else cache_key(name))
  if r in (None, b"", ""): return None
  return pickle.loads(r) if pickled else r.decode(encoding) if encoding else r

async def get_cache_batch(names: list[str], pickled=False, encoding="", raw_key=False):
  keys = [(name if raw_key else cache_key(name)) for name in names]
  r = await redis.mget(*keys)
  res = {}
  for name, value in zip(names, r):
    if value is None:
      res[name] = None
    else:
      res[name] = pickle.loads(value) if pickled else value.decode(encoding) if encoding else value
  return res

async def get_or_set_cache(name: str, callback: callable, expiry=YEAR_SECONDS, pickled=False, encoding=""):
  key = cache_key(name)
  value = await get_cache(key)
  if value:
    return pickle.loads(value) if pickled else value.decode(encoding) if encoding else value
  else:
    value = callback() if not iscoroutinefunction(callback) else await callback()
    if iscoroutine(value):
      value = await value
    if value in (None, b"", ""):
      log_warn(f"Cache could not be rehydrated for key: {key}")
      return None
    await cache(key, value, expiry=expiry, pickled=pickled)
  return value

# pubsub
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

# status
def get_status_key(name: str) -> str:
  return f"{NS}:status:{name}"

async def get_resources():
  r = await redis.keys(cache_key("*"))
  return [key.decode().split(":")[-1] for key in r]

async def get_topics(with_subs=False):
  r = await redis.pubsub_channels(f"{NS}:*")
  chans = [chan.decode().split(":")[-1] for chan in r]
  if with_subs:
    async with redis.pipeline(transaction=True) as pipe:
      for chan in chans:
        pipe.pubsub_numsub(chan)
      return {chan: await pipe.execute() for chan in chans}
  return chans

async def hydrate_resources_status():

  resources: dict = state.config.to_dict()
  cached = set(await get_resources())
  streamed = set(await get_topics(with_subs=False))
  for r in resources.keys():
    resources[r]["cached"] = r in cached
    resources[r]["streamed"] = r in streamed
  return resources

async def get_resource_status():
  return await get_or_set_cache(get_status_key("resources"),
    callback=lambda: hydrate_resources_status(),
    expiry=60, pickled=True)
