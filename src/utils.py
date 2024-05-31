import re
from asyncio import iscoroutinefunction, new_event_loop
from time import strftime
from datetime import timedelta, datetime, UTC
from typing import Coroutine, Literal, Optional
from dateutil.relativedelta import relativedelta
from os import environ as env, urandom
from hashlib import md5, sha256
from argparse import ArgumentParser

from dotenv import find_dotenv, load_dotenv
from web3 import Web3

YEAR_SECONDS = round(3.154e+7)
LOGFILE = env.get("LOGFILE", "out.log")
LogLevel = Literal["INFO", "ERROR", "DEBUG", "WARN"]

def log(level: LogLevel="INFO", *args):
  body = ' '.join(str(arg) for arg in args)
  msg = f"[{strftime('%Y-%m-%d %H:%M:%S')}] {level}: {body}"
  print(msg)
  with open(LOGFILE, "a+") as log:
    log.write(msg + "\n")

def log_debug(*args): log("DEBUG", *args); return True
def log_info(*args): log("INFO", *args); return True
def log_error(*args): log("ERROR", *args); return False
def log_warn(*args): log("WARN", *args); return False

def is_bool(value: any) -> bool:
  return str(value).lower() in ["true", "false", "yes", "no", "1", "0"]

def to_bool(value: str) -> bool:
  return value.lower() in ["true", "yes", "1"]

class ArgParser(ArgumentParser):

  info: dict[str, tuple] = {}
  parsed: any = None

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.info = {}  # Internal map for storing argument info
    self.parsed: any = None

  def add_argument(self, *args, **kwargs):
    action = super().add_argument(*args, **kwargs)
    if not action.type:
      action.type = bool if is_bool(action.const or action.default) else (type(action.default) or str)
    self.info[action.dest] = (action.type, action.default) # arg type and default value
    return action

  def get_info(self, arg_name):
    return self.info.get(arg_name)

  def parse_args(self, *args, **kwargs) -> any:
    self.parsed = super().parse_args(*args, **kwargs)
    return self.parsed

  def load_env(self, path: str=None) -> any:
    if not self.parsed:
      self.parse_args()
    load_dotenv(find_dotenv(path or self.parsed.env))
    for k, v in vars(self.parsed).items():
      arg_type, arg_default = self.get_info(k)
      is_default = arg_default == v
      env_val = env.get(k.upper())
      if env_val and is_default:
        selected = arg_type(env_val) if arg_type != bool else env_val.lower() == "true"
      else:
        selected = v
        env[k.upper()] = str(v) # inject into env for naive access
      setattr(self.parsed, k, selected)
    return self.parsed

CRON_BY_TF: dict[str, tuple] = {
  "s2": "* * * * * */2", # every 2 seconds
  "s5": "* * * * * */5", # every 5 seconds
  "s10": "* * * * * */10", # every 10 seconds
  "s15": "* * * * * */15", # every 15 seconds
  "s20": "* * * * * */20", # every 20 seconds
  "s30": "* * * * * */30", # every 30 seconds
  "m1": "*/1 * * * *", # every minute
  "m2": "*/2 * * * *", # every 2 minutes
  "m5": "*/5 * * * *", # every 5 minutes
  "m10": "*/10 * * * *", # every 10 minutes
  "m15": "*/15 * * * *", # every 15 minutes
  "m30": "*/30 * * * *", # every 30 minutes
  "h1": "0 * * * *", # every hour
  "h2": "0 */2 * * *", # every 2 hours
  "h4": "0 */4 * * *", # every 4 hours
  "h6": "0 */6 * * *", # every 6 hours
  "h8": "0 */8 * * *", # every 8 hours
  "h12": "0 */12 * * *", # every 12 hours
  "D1": "0 0 */1 * *", # every day
  "D2": "0 0 */2 * *",  # approx. every 2 days (odd days)
  "D3": "0 0 */3 * *",  # approx. every 3 days (multiple of 3)
  "W1": "0 0 * * 0", # every week (sunday at midnight)
  "M1": "0 0 1 */1 *", # every month (1st of the month)
  "Y1": "0 0 1 1 *", # every year (Jan 1)
}

SEC_BY_TF: dict[str, int] = {
  "s2": 2,
  "s5": 5,
  "s10": 10,
  "s15": 15,
  "s20": 20,
  "s30": 30,
  "m1": 60,
  "m2": 120,
  "m5": 300,
  "m10": 600,
  "m15": 900,
  "m30": 1800,
  "h1": 3600,
  "h2": 7200,
  "h4": 14400,
  "h6": 21600,
  "h8": 28800,
  "h12": 43200,
  "D1": 86400,
  "D2": 172800,
  "D3": 259200,
  "W1": 604800,
  "M1": 2592000,
  "Y1": 31536000,
}

INTERVAL_TO_SQL: dict[str, str] = {
  "m1": "1 minute",
  "m2": "2 minutes",
  "m5": "5 minutes",
  "m10": "10 minutes",
  "m15": "15 minutes",
  "m30": "30 minutes",
  "h1": "1 hour",
  "h2": "2 hours",
  "h4": "4 hours",
  "h6": "6 hours",
  "h8": "8 hours",
  "h12": "12 hours",
  "D1": "1 day",
  "D2": "2 days",
  "D3": "3 days",
  "W1": "1 week",
  "M1": "1 month",
  "Y1": "1 year"
}

def interval_to_sql(interval: str) -> str:
  return INTERVAL_TO_SQL.get(interval, None)

def interval_to_cron(interval: str) -> str:
  cron = CRON_BY_TF.get(interval, None)
  if not cron:
    raise ValueError(f"Invalid interval: {interval}")
  return cron

delta_by_unit: dict[str, callable] = {
  "s": lambda n: timedelta(seconds=n),
  "m": lambda n: timedelta(minutes=n),
  "h": lambda n: timedelta(hours=n),
  "D": lambda n: timedelta(days=n),
  "W": lambda n: timedelta(weeks=n),
  "M": lambda n: relativedelta(months=n),
  "Y": lambda n: relativedelta(years=n),
}

def extract_time_unit(interval: str) -> str:
  match = re.match(r"(\d+)([a-zA-Z]+)", interval)
  if match:
    value, unit = match.groups()
    return unit, int(value)
  else:
    raise ValueError(f"Invalid time frame format: {interval}")

def interval_to_delta(interval: str, backwards=False) -> timedelta:
  pattern = r"([smhDWMY])(\d+)"
  match = re.match(pattern, interval)
  if not match:
    raise ValueError(f"Invalid time unit. Only {', '.join(delta_by_unit.keys())} are supported.")

  unit, n = match.groups()
  n = int(n)
  if backwards:
    n = -n

  delta = delta_by_unit.get(unit, None)
  if not delta:
    raise NotImplementedError(f"Unsupported time unit: {unit}, please reach out at the dev team.")
  return delta(n)

def interval_to_seconds(interval: str, raw=False) -> int:
  secs = SEC_BY_TF.get(interval, None)
  if not raw or secs >= 604800: # 1 week
    delta = interval_to_delta(interval)
    now = datetime.now()
    secs = round((now + delta - now).total_seconds())
  return secs

def date_interval_floor(interval: str, date: Optional[datetime]) -> datetime:
  if not date:
    date = datetime.now(UTC)
  interval_sec = interval_to_seconds(interval)
  epoch_sec = int(date.timestamp())
  floored_epoch = epoch_sec - (epoch_sec % interval_sec)
  floored_datetime = datetime.fromtimestamp(floored_epoch, date.tzinfo)
  return floored_datetime

def floor_utc(interval: str="m1") -> datetime:
  return date_interval_floor(interval, datetime.now(UTC))

def shift_date(timeframe, date: Optional[datetime], backwards=False):
  if not date:
    date = datetime.now(UTC)
  return date + interval_to_delta(timeframe, backwards)

def select_nested(selector: Optional[str], data: dict) -> any:

  # invalid selectors
  if selector and not isinstance(selector, str):
    log_error("Invalid selector. Please use a valid path string")
    return None

  if not selector or [".", "root"].count(selector.lower()) > 0:
    return data

  # optional starting dot and "root" keyword (case-insensitive)
  if selector.startswith("."):
    selector = selector[1:]

  current = data # base access
  segment_pattern = re.compile(r'([^.\[\]]+)(?:\[(\d+)\])?') # match selector segments eg. ".key" or ".key[index]"

  # loop through segments
  for match in segment_pattern.finditer(selector):
    key, index = match.groups()
    if key.isnumeric() and not index:
      key, index = None, int(key)
    # dict access
    if key and isinstance(current, dict):
      current = current.get(key)
    if not current:
      return log_warn(f"Key not found in dict: {key}")
    # list access
    if index is not None:
      index = int(index)
      if not isinstance(current, list) or index >= len(current):
        return log_error(f"Index out of range in dict.{key}: {index}")
      current = current[index]
  return current

def generate_hash(derive_from: Optional[str], length=32) -> str:
  if not derive_from:
    derive_from = datetime.now(UTC).isoformat()
  hash_fn = md5 if length <= 32 else sha256
  return hash_fn((str(derive_from) + urandom(64).hex()).encode()).hexdigest()[:length]

def run_async_in_thread(fn: Coroutine):
  loop = new_event_loop()
  try:
    return loop.run_until_complete(fn)
  except Exception as e:
    log_error(f"Failed to run async function in thread: {e}")
    loop.close()

def submit_to_threadpool(executor, fn, *args, **kwargs):
  if iscoroutinefunction(fn):
      return executor.submit(run_async_in_thread, fn(*args, **kwargs))
  return executor.submit(fn, *args, **kwargs)

def split_chain_addr(target: str) -> tuple[str|int, str]:
  tokens = target.split(":")
  n = len(tokens)
  if n == 1:
    tokens = ["1", tokens[0]] # default to ethereum L1
  if n > 2:
    raise ValueError(f"Invalid target format for evm: {target}, expected chain_id:address")
  return int(tokens[0]), Web3.to_checksum_address(tokens[1])
