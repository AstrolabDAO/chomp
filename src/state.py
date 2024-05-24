from asyncio import get_event_loop, Task, get_running_loop
import asyncio
from concurrent.futures import ThreadPoolExecutor
from os import environ as env
from web3 import Web3
from multicall import constants as mc_const
import yaml
from redis import Redis
from aiocron import Cron, crontab

from src.utils import generate_hash, interval_to_cron, log_debug, log_error, log_info
from src.model import Config, Tsdb, Collector, Interval, TsdbAdapter

proc_id = env.get("COLLECTOR_ID", f"collector-{generate_hash(16)}")
adapter = env.get("TSDB", "tdengine").lower()
max_tasks = int(env.get("MAX_TASKS", 16))
max_retries = int(env.get("MAX_RETRIES", 10))
retry_cooldown = int(env.get("RETRY_COOLDOWN", 5))
env = env
verbose: bool = True
cron_by_id: dict[str, Cron] = {}

_thread_pool: ThreadPoolExecutor = {}
async def get_thread_pool() -> ThreadPoolExecutor:
  global _thread_pool
  if not _thread_pool:
    _thread_pool = ThreadPoolExecutor(max_workers=4)
  return _thread_pool

_rpcs_by_chain: dict[str|int, str] = {}
def get_rpcs(chain_id: str | int, load_all=False) -> str:
  if load_all and not _rpcs_by_chain:
    _rpcs_by_chain.update({k[:-10]: v.split(",") for k, v in env.items() if k.endswith("_HTTP_RPCS")})

  if chain_id not in _rpcs_by_chain:
    rpc_env = env.get(f"{chain_id}_HTTP_RPCS")
    if not rpc_env:
      raise ValueError(f"Missing RPC endpoints for chain {chain_id} ({chain_id}_HTTP_RPCS environment variable not found)")
    _rpcs_by_chain[chain_id] = rpc_env.split(",")
  return _rpcs_by_chain[chain_id]

# TODO: PR these multicall constants upstream
mc_const.MULTICALL3_ADDRESSES[238] = "0xcA11bde05977b3631167028862bE2a173976CA11" # blast
mc_const.MULTICALL3_ADDRESSES[5000] = "0xcA11bde05977b3631167028862bE2a173976CA11" # mantle
mc_const.MULTICALL3_ADDRESSES[59144] = "0xcA11bde05977b3631167028862bE2a173976CA11" # linea
mc_const.MULTICALL3_ADDRESSES[534352] = "0xcA11bde05977b3631167028862bE2a173976CA11" # scroll
# mc_const.GAS_LIMIT = 100_000_000

_web3_client_by_chain: dict[str | int, list[Web3]] = {}
_next_rpc_index_by_chain: dict[str | int, list[str]] = {}
async def get_web3_client(chain_id: str | int, rolling=True) -> Web3:
  global _web3_client_by_chain
  if not chain_id in _web3_client_by_chain:
    rpcs = get_rpcs(chain_id=chain_id)
    if not rpcs:
      raise ValueError(f"Missing RPC endpoints for chain {chain_id}")
    _web3_client_by_chain[chain_id] = []
    for rpc in rpcs:
      c = Web3(Web3.HTTPProvider("https://" + rpc))
      if c.is_connected():
        _web3_client_by_chain[chain_id].append(c) # only append to the rolling connections if connected
    if not _web3_client_by_chain[chain_id]:
      raise ValueError(f"Could not connect to chain {chain_id} using any of {rpcs}, skipping...")

  index = _next_rpc_index_by_chain.get(chain_id, 0)

  if rolling:
    _next_rpc_index_by_chain[chain_id] = (index + 1) % len(_web3_client_by_chain[chain_id])
  return _web3_client_by_chain[chain_id][index]

def get_loop():
  return get_event_loop()

class TsdbProxy:
  def __init__(self):
    self._tsdb = None

  @property
  def tsdb(self) -> Tsdb:
    if not self._tsdb:
      raise ValueError("No TSDB Adapter found")
    # get_loop().run_until_complete(self._tsdb.ensure_connected())
    return self._tsdb

  def set_adapter(self, db: Tsdb):
    self._tsdb = db

  def __getattr__(self, name):
    return getattr(self.tsdb, name)

tsdb = TsdbProxy()

class RedisProxy:
  def __init__(self):
    self._redis = None

  @property
  def redis(self) -> Redis:
    if not self._redis or not self._redis.ping():
      self._redis = Redis(
        host=env.get("REDIS_HOST", "localhost"),
        port=int(env.get("REDIS_PORT", 6379)),
        username=env.get("DB_RW_USER", "rw"),
        password=env.get("DB_RW_PASS", "pass"),
        db=int(env.get("REDIS_DB", 0))
      )
    return self._redis

  def __getattr__(self, name):
    return getattr(self.redis, name)

redis = RedisProxy()

class ConfigProxy:
  def __init__(self):
    self._config = None

  @staticmethod
  def load_config(path: str) -> Config:
    with open(path, 'r') as f:
      config_data = yaml.safe_load(f)
    return Config.from_dict(config_data)

  @property
  def config(self) -> Config:
    if not self._config:
      self._config = self.load_config(env.get("CONFIG_PATH", "collectors.yml"))
    return self._config

  def __getattr__(self, name):
    return getattr(self.config, name)

config = ConfigProxy()

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
