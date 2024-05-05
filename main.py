import argparse

from dotenv import find_dotenv, load_dotenv
import yaml
from arq import create_pool, cron
from arq.connections import RedisSettings
from arq.worker import Worker
import asyncio
from src.model import CollectorType, Config, Resource, CollectorConfig, CollectorConfig
from src.collectors.scrapper import collect as scrape_collect
from src.collectors.api import collect as fetch_collect
from src.collectors.evm import collect as evm_call_collect


# Helper function to load the configuration
def load_config(filepath: str) -> Config:
  with open(filepath, 'r') as file:
    config_data = yaml.safe_load(file)
  return Config.from_dict(config_data)

COLLECTOR_BY_TYPE: dict[CollectorType, callable] = {
  CollectorType.SCRAPPER: scrape_collect,
  CollectorType.API: fetch_collect,
  CollectorType.EVM: evm_call_collect
}

def main():
  pass
  # TODO:
  # 1. compare config collector configs with arq registered jobs
  # 2. delete dangling jobs, update or create when necessary based on unique jobId hash(name, target, selector, parameters, interval)
  # 3. start the worker to join the worker pool and work on the updated jobs
  # NB: each job should only be worked on once per interval (based on JobId hash)

if __name__ == "__main__":
  argparser = argparse.ArgumentParser(description="The collector retrieves, transforms and archives data from various sources.")
  argparser.add_argument("--env", default=".env.test", help="Environment file")
  args = argparser.parse_args()
  env_file = find_dotenv(args.env)
  load_dotenv(env_file)
  main()
