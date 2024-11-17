from concurrent.futures import ThreadPoolExecutor
import yamale
from os import cpu_count, environ as env
from web3 import Web3
from redis.asyncio import Redis, ConnectionPool

from src.utils import log_error
from src.model import Config, Tsdb

args: any
thread_pool: ThreadPoolExecutor

class ThreadPoolProxy:
  def __init__(self):
    self._thread_pool = None

  @property
  def thread_pool(self) -> ThreadPoolExecutor:
    if not self._thread_pool:
      self._thread_pool = ThreadPoolExecutor(max_workers=cpu_count() if args.threaded else 2)
    return self._thread_pool

  def __getattr__(self, name):
    return getattr(self.thread_pool, name)

class Web3Proxy:
  def __init__(self):
    self._by_chain = {}
    self._next_index_by_chain = {}
    self._rpcs_by_chain = {}

  def rpcs(self, chain_id: str | int, load_all=False) -> dict[str | int, list[str]]:
    if load_all and not self._rpcs_by_chain:
      self._rpcs_by_chain.update({k[:-10]: v.split(",") for k, v in env.items() if k.startswith("HTTP_RPCS")})

    if chain_id not in self._rpcs_by_chain:
      rpc_env = env.get(f"HTTP_RPCS_{chain_id}")
      if not rpc_env:
        raise ValueError(f"Missing RPC endpoints for chain {chain_id} (HTTP_RPCS_{chain_id} environment variable not found)")
      self._rpcs_by_chain[chain_id] = rpc_env.split(",")
    return self._rpcs_by_chain[chain_id]

  def client(self, chain_id: str | int, rolling=True) -> Web3:
    if chain_id not in self._by_chain:
      rpcs = self.rpcs(chain_id)
      if not rpcs:
        raise ValueError(f"Missing RPC endpoints for chain {chain_id}")
      self._by_chain[chain_id] = []
      for rpc in rpcs:
        c = Web3(Web3.HTTPProvider("https://" + rpc))
        if c.is_connected():
          self._by_chain[chain_id].append(c)
      if not self._by_chain[chain_id]:
        raise ValueError(f"Could not connect to chain {chain_id} using any of {rpcs}, skipping...")
    index = self._next_index_by_chain.get(chain_id, 0)
    if rolling:
      self._next_index_by_chain[chain_id] = (index + 1) % len(self._by_chain[chain_id]) # rotate proxy
    return self._by_chain[chain_id][index]

  # def __getattr__(self, name):
  #   return getattr(self.client(1), name)

class TsdbProxy:
  def __init__(self):
    self._tsdb = None

  @property
  def tsdb(self) -> Tsdb:
    if not self._tsdb:
      raise ValueError("No TSDB_ADAPTER Adapter found")
    # get_loop().run_until_complete(self._tsdb.ensure_connected())
    return self._tsdb

  def set_adapter(self, db: Tsdb):
    self._tsdb = db

  def __getattr__(self, name):
    return getattr(self.tsdb, name)

class RedisProxy:
  def __init__(self):
    self._pool = None
    self._redis = None

  @property
  def redis(self) -> Redis:
    if not self._redis:
      if not self._pool:
        self._pool = ConnectionPool(
          host=env.get("REDIS_HOST", "localhost"),
          port=int(env.get("REDIS_PORT", 6379)),
          username=env.get("DB_RW_USER", "rw"),
          password=env.get("DB_RW_PASS", "pass"),
          db=int(env.get("REDIS_DB", 0)),
          max_connections=int(env.get("REDIS_MAX_CONNECTIONS", 2 ** 16)),
        )
      self._redis = Redis(connection_pool=self._pool)
    return self._redis

  async def close(self):
    if self._redis:
      await self._redis.close()
      self._redis = None
    if self._pool:
      await self._pool.disconnect()
      self._pool = None

  def __getattr__(self, name):
    return getattr(self.redis, name)

class ConfigProxy:
  def __init__(self, _args):
    global args
    args = _args
    self._config = None

  @staticmethod
  def load_config(path: str) -> Config:
    schema = yamale.make_schema("./src/config-schema.yml")
    config_data = yamale.make_data(path)
    try:
      validation = yamale.validate(schema, yamale.make_data(path))
    except yamale.YamaleError as e:
      msg = ""
      for result in e.results:
        msg += f"Error validating {result.data} with schema {result.schema}\n"
        for error in result.errors:
          msg += f" - {error}\n"
      log_error(msg)
      exit(1)
    return Config.from_dict(config_data[0][0])

  @property
  def config(self) -> Config:
    if not self._config:
      self._config = self.load_config(args.config_path)
    return self._config

  def __getattr__(self, name):
    return getattr(self.config, name)
