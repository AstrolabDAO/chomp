from datetime import timedelta, now
from dateutil.relativedelta import relativedelta
import re
import time
from os import environ as env
from hashlib import md5
from model import CollectorConfig, Interval, ResourceField

LOGFILE = env.get("LOGFILE", "out.log")

def log(message, level="INFO"):
  timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
  msg = f"[{timestamp}] {level}: {message}"
  print(msg)
  with open(LOGFILE, "a+") as log:
    log.write(msg + "\n")

CRON_BY_TF: dict[Interval, tuple] = {
  "s2": "*/2 * * * * *",  # every 2 seconds
  "s5": "*/5 * * * * *",  # every 5 seconds
  "s10": "*/10 * * * * *",  # every 10 seconds
  "s20": "*/20 * * * * *",  # every 20 seconds
  "s30": "*/30 * * * * *",  # every 30 seconds
  "m1": "*/1 * * * *",  # every minute
  "m2": "*/2 * * * *",  # every 2 minutes
  "m5": "*/5 * * * *",  # every 5 minutes
  "m10": "*/10 * * * *",  # every 10 minutes
  "m15": "*/15 * * * *",  # every 15 minutes
  "m30": "*/30 * * * *",  # every 30 minutes
  "h1": "* */1 * * *",  # every hour
  "h2": "* */2 * * *",  # every 2 hours
  "h4": "* */4 * * *",  # every 4 hours
  "h6": "* */6 * * *",  # every 6 hours
  "h8": "* */8 * * *",  # every 8 hours
  "h12": "* */12 * * *",  # every 12 hours
  "D1": "* * */1 * *",  # every day
  "D2": "* * */2 * *",  # every 2 days
  "D3": "* * */3 * *",  # every 3 days
  "W1": "* * * */7 *",  # every week
  "M1": "* * 1 */1 *",  # every month
  "Y1": "* * 1 1 *",  # every year
}

def interval_to_cron(interval: Interval):
  cron = CRON_BY_TF.get(interval, None)
  if not cron:
    raise ValueError(f"Invalid interval: {interval}")
  return cron

def interval_to_delta(timeframe, backwards=False):
  pattern = r"([smhDWMY])(\d+)"
  match = re.match(pattern, timeframe)
  if not match:
    raise ValueError("Invalid timeframe format. Use 's', 'm', 'h', 'd', 'W', 'M', 'Y' followed by a number.")

  unit, n = match.groups()
  n = int(n)
  if backwards:
    n = -n

  if unit == 's':
    return relativedelta(seconds=n)
  elif unit == 'm':
    return relativedelta(minutes=n)
  elif unit == 'h':
    return relativedelta(hours=n)
  elif unit == 'd':
    return relativedelta(days=n)
  elif unit == 'M':
    return relativedelta(months=n)
  elif unit == 'Y':
    return relativedelta(years=n)
  else:
    raise ValueError("Invalid time unit. Only 's', 'm', 'h', 'd', 'M', 'Y' are supported.")

def interval_to_sql(interval: Interval) -> str:
  return interval[1] + interval[0];

def shift_date(timeframe, date=now(), backwards=False):
  return date + interval_to_delta(timeframe, backwards)

def field_signature(f: ResourceField) -> str:
  return f"{f.name}-{f.type}-{f.target}-{f.selector}-{','.join(f.parameters)}-{','.join(f.transformers)}"

def field_id(f: ResourceField) -> str:
  return md5(field_signature(f).encode()).hexdigest()

def collector_signature(c: CollectorConfig) -> str:
  return f"{c.name}-{c.resource_type}-{c.interval}-{c.collector_type}-{c.inplace}"\
    + "-".join([field_signature(f) for f in c.data])

def collector_id(c: CollectorConfig) -> str:
  return md5(collector_signature(c).encode()).hexdigest()
