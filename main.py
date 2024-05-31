from asyncio import gather, run
from typing import Type

from src.utils import ArgParser, generate_hash, log_debug, log_error, log_info, log_warn
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

  tsdb_class = get_adapter_class(state.args.tsdb_adapter)
  if not tsdb_class:
    raise ValueError(f"Missing or unsupported TSDB_ADAPTER adapter: {state.tsdb_adapter}, please use one of {', '.join([a.value for a in TsdbAdapter])}")

  # connect to cluster's TSDB_ADAPTER
  state.tsdb.set_adapter(await tsdb_class.connect())

  # load collectors configurations
  config = state.config
  collectors = config.scrapper + config.http_api + config.ws_api + config.evm_caller + config.evm_logger # + config.fix_api

  # running collectors/workers integrity check
  await state.check_collectors_integrity(collectors)

  # identify tasks left unclaimed by workers
  unclaimed = [c for c in collectors if not await is_task_claimed(c)]

  # cap the worker async tasks to the max allowed
  in_range = unclaimed[:state.args.max_jobs]

  # cli welcome message
  start_msg = """
        __
   ____/ /  ___  __ _  ___
  / __/ _ \/ _ \/  ' \/ _ \\
  \__/_//_/\___/_/_/_/ .__/
           ingester /_/ v0.1
+--------------------------+----------------+----------+--------+-----------+-----------------+
| Resource Name            | Collector      | Interval | Fields | Claimable | Picked-up       |
+--------------------------+----------------+----------+--------+-----------+-----------------+\n"""
  claims = 0
  for c in collectors:
    if c in in_range:
      claims += 1
    claim_str = f"({claims}/{state.args.max_jobs})".ljust(8, ' ')
    start_msg += f"| {c.name.ljust(24, ' ')[:24]} | {c.collector_type.ljust(15, ' ')}| {c.interval.ljust(8, ' ')} | {str(len(c.fields)).rjust(6, ' ')} "\
      + f"| {'yes 🟢' if c in unclaimed else 'no 🔴 '}    "\
      + f"| {f'yes 🟢 {claim_str}' if c in in_range else 'no 🔴'.ljust(14, ' ')} |\n"
  start_msg += "+--------------------------+----------------+----------+--------+-----------+-----------------+"
  log_info(start_msg)

  # schedule all tasks
  tasks = []
  for c in in_range:
    tasks.append(schedule(c))

  # schedule all at once
  await gather(*tasks)

  # start all crons (1 per interval, no more), multi-threaded mode avoids allows for parallel IO with sync adapters (eg. tdengine)
  cron_monitors = await state.scheduler.start(threaded=state.args.threaded)

  # run all tasks concurrently until interrupted (restarting is currently handled at the supervisor/container level)
  if not cron_monitors:
    log_warn(f"No cron scheduled, {len(collectors)} picked up by other workers. Shutting down...")
    return
  try:
    await gather(*cron_monitors)
  except KeyboardInterrupt:
    log_info("Shutting down...")
  finally:
    # gracefully close connections to the TSDB_ADAPTER and cache
    if state.tsdb:
      await state.tsdb.close()
    if state.redis:
      await state.redis.close()

if __name__ == "__main__":
  ap = ArgParser(description="Chomp retrieves, transforms and archives data from various sources.")
  ap.add_argument("-e", "--env", type=str, default=".env", help="Environment file if any")
  ap.add_argument("-c", "--config_path", type=str, default="./examples/dex-vs-cex.yml", help="Collectors YAML configuration file")
  ap.add_argument("-v", "--verbose", default=False, action='store_true', help="Verbose output (loglevel debug)")
  ap.add_argument("-i", "--proc_id", type=str, default=f"chomp-{generate_hash(16)}", help="Unique instance identifier")
  ap.add_argument("-a", "--tsdb_adapter", type=str, default="tdengine", help="Timeseries database adapter")
  ap.add_argument("-j", "--max_jobs", type=int, default=16, help="Max collector jobs to run concurrently")
  ap.add_argument("-r", "--max_retries", type=int, default=5, help="Max collector retries per event, applies to fetching/querying")
  ap.add_argument("-co", "--retry_cooldown", type=int, default=2, help="Min sleep time between retries, in seconds")
  ap.add_argument("-t", "--threaded", default=True, action='store_true', help="Run jobs in separate threads")
  ap.add_argument("-p", "--perpetual_indexing", default=False, action='store_true', help="Perpetually listen for new blocks to index, requires capable RPCs")
  state.args = ap.load_env()
  # reload(state) # update state import to include .env values
  run(main())
