import argparse
import asyncio
from typing import Type
from dotenv import find_dotenv, load_dotenv
from importlib import reload

from src.utils import log_debug, log_error, log_info
from src.model import Tsdb, TsdbAdapter
import src.state as state
from src.cache import is_task_claimed
from src.actions.schedule import schedule

def get_adapter_class(adapter: TsdbAdapter) -> Type[Tsdb]:
  import src.adapters as adapters
  implementations: dict[TsdbAdapter, Type[Tsdb]] = {
    "tdengine": adapters.tdengine.Taos,
    # "timescale": adapters.timescale.Timescale,
    # "influx": adapters.influx.Influx,
  }
  return implementations.get(adapter.lower(), None)

async def main():
  # reload state to fetch .env config updates
  reload(state)
  tsdb_class = get_adapter_class(state.adapter)
  if not tsdb_class:
    raise ValueError(f"Missing or unsupported TSDB adapter: {state.adapter}, please use one of {', '.join([a.value for a in TsdbAdapter])}")

  # connect to cluster's TSDB
  state.tsdb = await tsdb_class.connect()
  # load collector configurations
  config = state.get_config()
  collectors = config.scrapper + config.http_api + config.ws_api # + config.fix_api + config.evm
  # running collectors/workers integrity check
  await state.check_collectors_integrity(collectors)
  # identify tasks left unclaimed by workers
  unclaimed = list(filter(lambda c: not is_task_claimed(c), collectors))
  # cap the worker async tasks to the max allowed
  in_range = unclaimed[:state.max_tasks]
  # cli welcome message
  start_msg = """
  _____ _             ___       _ _           _
 |_   _| |__   ___   / __| ___ | | | ___  ___| |_ ___  _ _
   | | | '_ \ / , \ | |   /   \| | |/ , \/ __| ._/   \| '_|
   | | | | | |  __/ | |__|  O  | | |  __| (__| |_' O  | |
   |_| |_| |_|\___|  \____\___/|_|_|\___,\___|__,\___/|_|

  Collector Name\t| Type\t\t| Interval\t| Fields\t| Status\t| Worker Availability
  -----------------------------------------------------------------------------------------------------------\n"""
  claims = 0
  for c in collectors:
    if c in in_range:
      claims += 1
    start_msg += f"  {c.name}\t| {c.collector_type}\t| {c.interval}\t\t| {len(c.data)} fields\t"\
      + f"| {'unclaimed 🟢' if c in unclaimed else 'claimed 🔴'}\t"\
      + f"| {'in range 🟢' if c in in_range else 'out of range 🔴'} ({claims}/{state.max_tasks})\n"
  log_info(start_msg)
  # schedule all tasks
  tasks = []
  for c in in_range:
    tasks += await schedule(c)
  log_info(f"Starting {len(tasks)} tasks ({len(in_range)} collector crons, {len(tasks) - len(in_range)} extraneous tasks)")
  # run all tasks concurrently until interrupted (restarting is currently handled at the supervisor/container level)
  try:
    await asyncio.gather(*tasks)
  except KeyboardInterrupt:
    log_info("Shutting down...")
  # except Exception as e:
  #   log_error(f"Unexpected error: {e}")
  # gracefully close connections to the TSDB and cache
  finally:
    if state.tsdb:
      await state.tsdb.close()
    if state.redis:
      state.redis.close()

if __name__ == "__main__":
  argparser = argparse.ArgumentParser(description="The Collector retrieves, transforms and archives data from various sources.")
  argparser.add_argument("-e", "--env", default=".env.test", help="Environment file")
  argparser.add_argument("-v", "--verbose", default=True, help="Verbose output")
  args = argparser.parse_args()
  state.verbose = args.verbose
  load_dotenv(find_dotenv(args.env))
  asyncio.run(main())
