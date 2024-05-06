import redis
from os import environ as env
from src.model import Collector, Interval
from src.state import get_redis
from src.utils import YEAR_SECONDS, log_warn

NS = env.get("REDIS_NS", "collector")

def resource_key(name: str, interval: Interval) -> str:
  return f"{NS}:{name}:{interval}"

def claim_resource(name="sync:2fa:stop"):
  return get_redis().set(name, "true")

def is_resource_claimed(name="sync:2fa:stop"):
  return get_redis().exists(name) == 1

def free_resource(name="sync:2fa:stop"):
  return get_redis().delete(name)

def cache(key: str, value: str|int|float|bool, expiry: int=YEAR_SECONDS):
  get_redis().setex(key, expiry, value)

def get_cache(key: str):
  return get_redis().get(key)

def get_or_set_cache(key: str, callback: callable, expiry: int):
  value = get_cache(key)
  if not value:
    value = callback()
    if value == None or value == "":
      log_warn(f"Cache could not be rehydrated for key: {key}")
      return None
    cache(key, value, expiry)
  return value
