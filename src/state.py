from fastapi import FastAPI
from multicall import constants as mc_const

from src.utils.format import log_info
from src.utils import PackageMeta
from src.utils.proxies import ThreadPoolProxy, Web3Proxy, TsdbProxy, RedisProxy, ConfigProxy

args: any
meta = PackageMeta(package="chomp")
server: FastAPI

tsdb: TsdbProxy
redis: RedisProxy
config: ConfigProxy
web3: Web3Proxy
thread_pool: ThreadPoolProxy

def init(args_: any):
  global args, meta, thread_pool, rpcs, web3, tsdb, redis, config
  args = args_
  config = ConfigProxy(args)
  thread_pool = ThreadPoolProxy()
  tsdb = TsdbProxy()
  redis = RedisProxy()
  web3 = Web3Proxy()

# TODO: PR these multicall constants upstream
mc_const.MULTICALL3_ADDRESSES[238] = "0xcA11bde05977b3631167028862bE2a173976CA11" # blast
mc_const.MULTICALL3_ADDRESSES[5000] = "0xcA11bde05977b3631167028862bE2a173976CA11" # mantle
mc_const.MULTICALL3_ADDRESSES[59144] = "0xcA11bde05977b3631167028862bE2a173976CA11" # linea
mc_const.MULTICALL3_ADDRESSES[534352] = "0xcA11bde05977b3631167028862bE2a173976CA11" # scroll
mc_const.GAS_LIMIT = 5_000_000
