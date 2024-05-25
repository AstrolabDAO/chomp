import argparse
import asyncio
from typing import Type
from dotenv import find_dotenv, load_dotenv
from importlib import reload

from src.utils import log_debug, log_error, log_info, log_warn
from src.model import Tsdb, TsdbAdapter
import src.state as state
from src.cache import is_task_claimed
from src.actions.schedule import schedule

def get_adapter_class(adapter: TsdbAdapter) -> Type[Tsdb]:
  import src.adapters as adapters
  implementations: dict[TsdbAdapter, Type[Tsdb]] = {
    "tdengine": adapters.tdengine.Taos,
    # "timescale": adapters.timescale.TimescaleDb,
    # "opentsdb": adapters.opentsdb.OpenTsdb,
    # "questdb": adapters.questdb.QuestDb,
    # "mongodb": adapters.mongodb.MongoDb,
    # "duckdb": adapters.duckdb.DuckDb,
    # "influxdb": adapters.influxdb.InfluxDb,
    # "clickhouse": adapters.clickhouse.ClickHouse,
    # "victoriametrics": adapters.victoriametrics.VictoriaMetrics,
    # "kx": adapters.kx.Kdb,
  }
  return implementations.get(adapter.lower(), None)

async def main():
  reload(state) # reload state to fetch .env config updates parsed after argparse

  tsdb_class = get_adapter_class(state.adapter)
  if not tsdb_class:
    raise ValueError(f"Missing or unsupported TSDB adapter: {state.adapter}, please use one of {', '.join([a.value for a in TsdbAdapter])}")

  # connect to cluster's TSDB
  state.tsdb.set_adapter(await tsdb_class.connect())

# load collector configurations
  config = state.config
  collectors = config.scrapper + config.http_api + config.ws_api + config.evm # + config.fix_api

  # running collectors/workers integrity check
  await state.check_collectors_integrity(collectors)

  # identify tasks left unclaimed by workers
  unclaimed = [c for c in collectors if not await is_task_claimed(c)]

  # cap the worker async tasks to the max allowed
  in_range = unclaimed[:state.max_tasks]

  # cli welcome message
  start_msg = """
  _____ _             ___       _ _           _
 |_   _| |__   ___   / __| ___ | | | ___  ___| |_ ___  _ _
   | | | '_ \ / , \ | |   /   \| | |/ , \/ __| ._/   \| '_|
   | | | | | |  __/ | |__|  O  | | |  __| (__| |_' O  | |
   |_| |_| |_|\___|  \____\___/|_|_|\___,\___|__,\___/|_|

  Collector Name                | Type        | Interval | Fields    | Status           | Pickup Status
 -----------------------------------------------------------------------------------------------------------------\n"""
  claims = 0
  for c in collectors:
    if c in in_range:
      claims += 1
    start_msg += f"  {c.name.ljust(30, ' ')}| {c.collector_type.ljust(12, ' ')}| {c.interval.ljust(9, ' ')}| {str(len(c.fields)).rjust(2, '>')} fields "\
      + f"| {'unclaimed 🟢' if c in unclaimed else 'claimed 🔴'}\t"\
      + f"| {f'picked up 🟢 ({claims}/{state.max_tasks})' if c in in_range else 'not picked up 🔴'}\n"
  log_info(start_msg)

  # schedule all tasks
  tasks = []
  for c in in_range:
    tasks.append(schedule(c))

  # schedule all at once
  await asyncio.gather(*tasks)

  # start all crons (1 per interval, no more), multi-threaded mode avoids allows for parallel IO with sync adapters (eg. tdengine)
  cron_monitors = await state.scheduler.start(threaded=state.threaded)

  # run all tasks concurrently until interrupted (restarting is currently handled at the supervisor/container level)
  if not cron_monitors:
    log_warn(f"No cron scheduled, {len(collectors)} picked up by other workers. Shutting down...")
    return
  try:
    await asyncio.gather(*cron_monitors)
  except KeyboardInterrupt:
    log_info("Shutting down...")
  finally:
    # gracefully close connections to the TSDB and cache
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
