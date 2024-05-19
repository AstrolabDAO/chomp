from asyncio import get_event_loop, Task, get_running_loop
from os import environ as env
import yaml
from redis import Redis
from aiocron import Cron, crontab

from src.utils import generate_hash, interval_to_cron, log_debug, log_error, log_info
from src.model import Config, Tsdb, Collector, Interval, TsdbAdapter

proc_id = env.get("COLLECTOR_ID", f"collector-{generate_hash(16)}")
adapter = env.get("TSDB", "tdengine").lower()
max_tasks = int(env.get("MAX_TASKS", 16))
env = env
config: Config = {}
redis: Redis = {}
tsdb: Tsdb = {}
verbose: bool = False
cron_by_id: dict[str, Cron] = {}

def get_loop():
  return get_event_loop()

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

async def monitor_cron(cron: Cron):
  while True:
    try:
      await cron.next()
    except Exception as e:
      log_error(f"Cron job failed with exception: {e}")
      get_running_loop().stop()
      break

async def check_collectors_integrity(collectors: list[Collector]):
  log_debug("TODO: implement collectors integrity check (eg. if claimed resource, check last collection time+tsdb table schema vs resource schema...)")

async def add_cron(id: str, fn: callable, args: list, interval: Interval="h1") -> Task:
  global cron_by_id
  cron_by_id[id] = crontab(interval_to_cron(interval), func=fn, args=args)
  log_info(f"Added cron job {id} at interval {interval} to {proc_id}")
  return await monitor_cron(cron_by_id[id])
