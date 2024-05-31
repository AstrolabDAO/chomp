from asyncio import get_event_loop, Task, get_running_loop, gather
from concurrent.futures import ThreadPoolExecutor
import yamale
from os import cpu_count, environ as env
from web3 import Web3
from multicall import constants as mc_const
from aiocron import Cron, crontab
from redis.asyncio import Redis, ConnectionPool # from redis.client import Redis for async client
from src.utils import interval_to_cron, log_debug, log_error, log_info, log_warn, submit_to_threadpool
from src.model import Config, Tsdb, Collector, Interval, TsdbAdapter

args: any = None

# TODO: wrap in proxy
_thread_pool: ThreadPoolExecutor = {}
def get_thread_pool() -> ThreadPoolExecutor:
  global _thread_pool
  if not _thread_pool:
    _thread_pool = ThreadPoolExecutor(max_workers=cpu_count() if args.threaded else 2)
  return _thread_pool

# TODO: wrap in proxy
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
mc_const.GAS_LIMIT = 5_000_000

# TODO: wrap in proxy
_web3_client_by_chain: dict[str | int, list[Web3]] = {}
_next_rpc_index_by_chain: dict[str | int, list[str]] = {}
def get_web3_client(chain_id: str | int, rolling=True) -> Web3:
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
      raise ValueError("No TSDB_ADAPTER Adapter found")
    # get_loop().run_until_complete(self._tsdb.ensure_connected())
    return self._tsdb

  def set_adapter(self, db: Tsdb):
    self._tsdb = db

  def __getattr__(self, name):
    return getattr(self.tsdb, name)

tsdb = TsdbProxy()

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

redis = RedisProxy()

class ConfigProxy:
  def __init__(self):
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

# TODO: merge with schedule.py
class Scheduler:
  def __init__(self):
    self.cron_by_job_id = {}
    self.cron_by_interval = {}
    self.jobs_by_interval = {}
    self.job_by_id = {}

  def run_threaded(self, job_ids: list[str]):
    jobs = [self.job_by_id[j] for j in job_ids]
    tp = get_thread_pool()
    ft = [submit_to_threadpool(tp, job[0], *job[1]) for job in jobs]
    return [f.result() for f in ft] # wait for all results

  async def run_async(self, job_ids: list[str]):
    jobs = [self.job_by_id[j] for j in job_ids]
    ft = [job[0](*job[1]) for job in jobs]
    return await gather(*ft)

  async def add(self, id: str, fn: callable, args: list, interval: Interval="h1", start=True, threaded=False) -> Task:
    if id in self.job_by_id:
      raise ValueError(f"Duplicate job id: {id}")
    self.job_by_id[id] = (fn, args)

    jobs = self.jobs_by_interval.setdefault(interval, [])
    jobs.append(id)

    if not start:
      return None

    return start(self, interval, threaded)

  async def start_interval(self, interval: Interval, threaded=False) -> Task:
    if interval in self.cron_by_interval:
      old_cron = self.cron_by_interval[interval]
      old_cron.stop() # stop prev cron

    job_ids = self.jobs_by_interval[interval]

    # cron = crontab(interval_to_cron(interval), func=self.run_threaded if threaded else self.run_async, args=(job_ids,))
    cron = crontab(interval_to_cron(interval), func=self.run_async, args=(job_ids,))

    # update the interval's jobs cron ref
    for id in job_ids:
      self.cron_by_job_id[id] = cron
    self.cron_by_interval[interval] = cron

    log_info(f"Proc {args.proc_id} starting {interval} {'threaded' if threaded else 'async'} cron with {len(job_ids)} jobs: {job_ids}")
    return await monitor_cron(self.cron_by_job_id[id])

  async def start(self, threaded=False) -> list[Task]:
    intervals, jobs = self.jobs_by_interval.keys(), self.job_by_id.keys()
    log_info(f"Starting {len(jobs)} jobs ({len(intervals)} crons: {list(intervals)})")
    return [self.start_interval(i, threaded) for i in self.jobs_by_interval.keys()]

  async def add_collector(self, c: Collector, fn: callable, start=True, threaded=False) -> Task:
    return await self.add(id=c.id, fn=fn, args=(c,), interval=c.interval, start=start, threaded=threaded)

  async def add_collectors(self, collectors: list[Collector], fn: callable, start=True, threaded=False) -> list[Task]:
    intervals = set([c.interval for c in collectors])
    added = await gather(*[self.add_collector(c, fn, start=False, threaded=threaded) for c in collectors])
    if start:
      await gather(*[self.start_interval(i, threaded) for i in intervals])

scheduler = Scheduler()
