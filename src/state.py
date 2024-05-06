from os import environ as env
import yaml
from redis import Redis
from src.model import Config, Tsdb

env = env
config: Config = None
redis: Redis = None
tsdb: Tsdb = None
verbose: bool = False

def get_tsdb() -> Tsdb:
  global tsdb
  if not tsdb:
    raise ValueError("No TSDB Adapter found")
  return tsdb

def get_redis() -> Redis:
  global redis
  if not redis or not redis.ping():
    redis = Redis(
      host=env.get("REDIS_HOST", "localhost"),
      port=int(env.get("REDIS_PORT", 6039)),
      username=env.get("DB_RW_USER", "rw"),
      password=env.get("DB_RW_PASS", "pass"),
      db=int(env.get("REDIS_DB", 0)))
  return redis

def load_config(path: str) -> Config:
  with open(path, 'r') as f:
    config_data = yaml.safe_load(f)
  return Config.from_dict(config_data)

def get_config() -> Config:
  global config
  if not config:
    config = load_config(env.get("CONFIG_PATH", "collectors.yml"))
  return config
