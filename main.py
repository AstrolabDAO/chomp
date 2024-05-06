# TODO in main():
# 1. compare collectors.yml config with arq registered jobs (eg. by versionning each collector config in Redis)
# 2. delete dangling jobs, update or create if necessary based on collector.id
# 3. start env.MAX_TASKS workers to join arq worker pool (max 16 per instance)
# 4: test arq cron scheduling + make sure jobs only run once and get picked up by new workers when prev fail

import argparse
from os import environ as env
from typing import Type
from dotenv import find_dotenv, load_dotenv
from arq import create_pool, cron
from arq.connections import RedisSettings
from arq.worker import Worker
from src.model import CollectorType, Config, Resource, Collector, Collector, Tsdb, TsdbAdapter
from src.actions.collect import collect
import src.state as state

def get_adapter(adapter: str) -> Type[Tsdb]:
  import src.adapters as adapters
  implementations: dict[TsdbAdapter, Type[Tsdb]] = {
    TsdbAdapter.TDENGINE: adapters.tdengine.Taos,
  }
  return implementations.get(TsdbAdapter(adapter.lower()), None)

def main():
  tsdb_class = get_adapter(env.get("TSDB", "").lower())
  if not tsdb_class:
    raise ValueError(f"Missing or unsupported TSDB adapter, please use one of {', '.join([a.value for a in TsdbAdapter])}")

  ### DEBUG: these are to be replaced by arq scheduling as defined in the file's TODO
  state.tsdb = tsdb_class.connect()
  collect({}, state.get_config().scrapper[0])
  collect({}, state.get_config().api[0])
  ###

if __name__ == "__main__":
  argparser = argparse.ArgumentParser(description="The Collector retrieves, transforms and archives data from various sources.")
  argparser.add_argument("-e", "--env", default=".env.test", help="Environment file")
  argparser.add_argument("-v", "--verbose", default=True, help="Verbose output")
  args = argparser.parse_args()
  state.verbose = args.verbose
  load_dotenv(find_dotenv(args.env))
  main()
