from asyncio import gather, run
from typing import Type

from src.utils import log_info, log_warn, ArgParser, generate_hash, prettify
from src.model import Config, Tsdb, TsdbAdapter
import src.state as state

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
  return implementations.get(adapter.lower(), bool)

async def start_ingester(config: Config):
  # ingester specific imports
  from src.cache import is_task_claimed
  from src.actions import schedule, scheduler, check_ingesters_integrity

  ingesters = config.ingesters
  await check_ingesters_integrity(ingesters)
  unclaimed = [c for c in ingesters if not await is_task_claimed(c)]
  in_range = unclaimed[:state.args.max_jobs]

  table_data = []
  claims = 0
  for c in ingesters:
    if c in in_range:
      claims += 1
    claim_str = f"({claims}/{state.args.max_jobs})"
    table_data.append([
      c.name[:21].ljust(24, ".") if len(c.name) > 24 else c.name, c.ingester_type, c.interval, len(c.fields),
      'yes 🟢' if c in unclaimed else 'no 🔴', f'yes 🟢 {claim_str}' if c in in_range else 'no 🔴'
    ])

  log_info(f'\n{prettify(table_data, ["Resource", "Ingester", "Interval", "Fields", "Claimable", "Picked-up"])}')

  tasks = [schedule(c) for c in in_range]
  await gather(*tasks)

  cron_monitors = await scheduler.start(threaded=state.args.threaded)
  if not cron_monitors:
    log_warn("No cron scheduled, tasks picked up by other workers. Shutting down...")
    return
  await gather(*cron_monitors)

async def start_server(config: Config):
  # server specific imports
  from src.server import start
  await start()

async def main(ap: ArgParser):
  tsdb_class = get_adapter_class(state.args.tsdb_adapter)
  if not tsdb_class:
    raise ValueError(
      f"Unsupported TSDB_ADAPTER adapter: {state.tsdb_adapter}. "
      f"Please use one of {[a.value for a in TsdbAdapter]}."
    )

  state.tsdb.set_adapter(await tsdb_class.connect())

  try:
    config = state.config
    await (start_server(config) if state.args.server else start_ingester(config))
  except KeyboardInterrupt:
    log_info("Shutting down...")
  finally:
    await state.tsdb.close()
    await state.redis.close()

if __name__ == "__main__":
  log_info(f"""
        __
   ____/ /  ___  __ _  ___
  / __/ _ \/ _ \/  ' \/ _ \\
  \__/_//_/\___/_/_/_/ .__/
           ingester /_/ v{state.meta.version_no_patch}\n\n""")
  ap = ArgParser(description="Chomp retrieves, transforms and archives data from various sources.")
  ap.add_groups({
    "Common runtime": [
      (("-e", "--env"), str, ".env", None, "Environment file if any"),
      (("-v", "--verbose"), bool, False, 'store_true', "Verbose output (loglevel debug)"),
      (("-i", "--proc_id"), str, f"chomp-{generate_hash(length=8)}", None, "Unique instance identifier"),
      (("-r", "--max_retries"), int, 5, None, "Max ingester retries per event, applies to fetching/querying"),
      (("-rc", "--retry_cooldown"), int, 2, None, "Min sleep time between retries, in seconds"),
      (("-t", "--threaded"), bool, True, 'store_true', "Run jobs/routers in separate threads"),
      (("-j", "--max_jobs"), int, 16, None, "Max ingester jobs to run concurrently"),
      (("-a", "--tsdb_adapter"), str, "tdengine", None, "Timeseries database adapter"),
      (("-c", "--config_path"), str, "./examples/dex-vs-cex.yml", None, "Ingesters YAML configuration file"),
    ],
    "Ingester runtime": [
      (("-p", "--perpetual_indexing"), bool, False, 'store_true', "Perpetually listen for new blocks to index, requires capable RPCs"),
    ],
    "Server runtime": [
      (("-s", "--server"), bool, False, 'store_true', "Run as server (ingester by default)"),
      (("-sh", "--host"), str, "127.0.0.1", None, "FastAPI server host"),
      (("-sp", "--port"), int, 8000, None, "FastAPI server port"),
      (("-wpi", "--ws_ping_interval"), int, 30, None, "Websocket server ping interval"),
      (("-wpt", "--ws_ping_timeout"), int, 20, None, "Websocket server ping timeout")
    ]})
  state.init(args_=ap.load_env())
  log_info(f"Arguments\n{ap.pretty()}")
  # reload(state) # update state import to include .env values
  run(main(ap))
