import redis
from os import environ as env

NS = env.get("REDIS_NS", "collector")

def resource_key(name: str, interval: Interval) -> str:
  return f"{NS}:{name}"

redis_client: redis.Redis = None
def get_redis():
  global redis_client
  if not redis_client or not redis_client.ping():
    redis_client = redis.Redis(
      host=env.get("REDIS_HOST", "localhost"),
      port=env.get("REDIS_PORT", 6039),
      db=int(env.get("REDIS_DB", 0)))
  return redis_client

def claim_resource(name="sync:2fa:stop"):
  return get_redis().set(name, "true")

def is_resource_claimed(name="sync:2fa:stop"):
  return get_redis().exists(name) == 1

def free_resource(name="sync:2fa:stop"):
  return get_redis().delete(name)
