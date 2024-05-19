import re
from time import strftime
from datetime import timedelta, datetime, UTC
from typing import Literal, Optional
from dateutil.relativedelta import relativedelta
from os import environ as env, urandom
from hashlib import md5, sha256

YEAR_SECONDS = round(3.154e+7)
LOGFILE = env.get("LOGFILE", "out.log")

LogLevel = Literal["INFO", "ERROR", "DEBUG", "WARN"]

def log(level: LogLevel="INFO", *args):
  body = ' '.join(str(arg) for arg in args)
  msg = f"[{strftime('%Y-%m-%d %H:%M:%S')}] {level}: {body}"
  print(msg)
  with open(LOGFILE, "a+") as log:
    log.write(msg + "\n")

def log_info(*args): log("INFO", *args)
def log_error(*args): log("ERROR", *args)
def log_debug(*args): log("DEBUG", *args)
def log_warn(*args): log("WARN", *args)

CRON_BY_TF: dict[str, tuple] = {
  "s2": "* * * * * */2",  # every 2 seconds
  "s5": "* * * * * */5",  # every 5 seconds
  "s10": "* * * * * */10",  # every 10 seconds
  "s20": "* * * * * */20",  # every 20 seconds
  "s30": "* * * * * */30",  # every 30 seconds
  "m1": "*/1 * * * *",  # every minute
  "m2": "*/2 * * * *",  # every 2 minutes
  "m5": "*/5 * * * *",  # every 5 minutes
  "m10": "*/10 * * * *",  # every 10 minutes
  "m15": "*/15 * * * *",  # every 15 minutes
  "m30": "*/30 * * * *",  # every 30 minutes
  "h1": "0 * * * *",  # every hour
  "h2": "0 */2 * * *",  # every 2 hours
  "h4": "0 */4 * * *",  # every 4 hours
  "h6": "0 */6 * * *",  # every 6 hours
  "h8": "0 */8 * * *",  # every 8 hours
  "h12": "0 */12 * * *",  # every 12 hours
  "D1": "0 0 */1 * *",  # every day
  "D2": "0 0 */2 * *",  # approx. every 2 days (odd days)
  "D3": "0 0 */3 * *",  # approx. every 3 days (multiple of 3)
  "W1": "0 0 * * 0",  # every week (sunday at midnight)
  "M1": "0 0 1 */1 *",  # every month (1st of the month)
  "Y1": "0 0 1 1 *",  # every year (Jan 1)
}

SEC_BY_TF: dict[str, int] = {
  "s2": 2,
  "s5": 5,
  "s10": 10,
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

def interval_to_cron(interval: str):
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

def interval_to_delta(timeframe, backwards=False):
  pattern = r"([smhDWMY])(\d+)"
  match = re.match(pattern, timeframe)
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

def date_interval_floor(interval: str, date=datetime.now(UTC)) -> datetime:
  interval_sec = interval_to_seconds(interval)
  epoch_sec = int(date.timestamp())
  floored_epoch = epoch_sec - (epoch_sec % interval_sec)
  floored_datetime = datetime.fromtimestamp(floored_epoch, date.tzinfo)
  return floored_datetime

def floor_utc(interval: str="m1") -> datetime:
  return date_interval_floor(interval, datetime.now(UTC))

def interval_to_sql(interval: str) -> str:
  return interval[1] + interval[0];

def shift_date(timeframe, date=datetime.now(UTC), backwards=False):
  return date + interval_to_delta(timeframe, backwards)

def select_from_dict(selector: Optional[str], data: dict) -> any:
  # Traverse the json with the selector
  if selector and selector not in {"", ".", "root"}:
    if selector.startswith("."):
      selector = selector[1:]
    for key in selector.split("."):
      data = data.get(key, None)
      if not data:
        log_error(f"Failed to find element {selector} in fetched json, skipping...")
        break
  return data

def generate_hash(derive_from=datetime.now(UTC), length=32) -> str:
  hash_fn = md5 if length <= 32 else sha256
  return hash_fn((str(derive_from) + urandom(64).hex()).encode()).hexdigest()[:length]
