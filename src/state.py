from asyncio import get_event_loop, Task, get_running_loop
from concurrent.futures import ThreadPoolExecutor
from os import environ as env
from web3 import Web3
import yaml
from redis import Redis
from aiocron import Cron, crontab

from src.utils import generate_hash, interval_to_cron, log_debug, log_error, log_info
from src.model import Config, Tsdb, Collector, Interval, TsdbAdapter

thread_pool: ThreadPoolExecutor = {}
proc_id = env.get("COLLECTOR_ID", f"collector-{generate_hash(16)}")
adapter = env.get("TSDB", "tdengine").lower()
max_tasks = int(env.get("MAX_TASKS", 16))
max_retries = int(env.get("MAX_RETRIES", 10))
retry_cooldown = int(env.get("RETRY_COOLDOWN", 5))
env = env
config: Config = {}
redis: Redis = {}
tsdb: Tsdb = {}
verbose: bool = True
cron_by_id: dict[str, Cron] = {}
rpc_by_chain: dict[str|int, str] = {}
web3_client_by_chain: dict[str|int, Web3] = {}

def get_thread_pool() -> ThreadPoolExecutor:
  global thread_pool
  if not thread_pool:
    thread_pool = ThreadPoolExecutor(max_workers=4)
  return thread_pool

def get_rpc(chain_id: str | int, load_all=False) -> str:
  if load_all and not rpc_by_chain:
    rpc_by_chain.update({k[:-4]: v for k, v in env.items() if k.endswith("_RPC")})

  rpc = rpc_by_chain.get(chain_id)
  if not rpc:
    rpc = env.get(f"{chain_id}_RPC")
    if not rpc:
      raise ValueError(f"Missing RPC endpoint for chain {chain_id} ({chain_id}_RPC environment variable not found)")
    rpc_by_chain[chain_id] = rpc

  return rpc

def get_web3_client(chain_id: str | int) -> Web3:
  global web3_client_by_chain
  c = web3_client_by_chain.get(chain_id)
  if not c:
    rpc = get_rpc(chain_id)
    if not rpc:
      raise ValueError(f"Missing RPC endpoint for chain {chain_id}")
    c = Web3(Web3.HTTPProvider(rpc))
  if not c.is_connected():
    raise ValueError(f"Could not connect to chain {chain_id} using {c.provider}, skipping...")
  web3_client_by_chain[chain_id] = c
  return c

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
