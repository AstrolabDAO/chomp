from datetime import UTC, datetime, timedelta
import re
from typing import Literal
from dateutil.relativedelta import relativedelta

# below are based on ISO 8601 capitalization (cf. https://en.wikipedia.org/wiki/ISO_8601)
TimeUnit = Literal["ns", "us", "ms", "s", "m", "h", "D", "W", "M", "Y"]
Interval = Literal[
  "s2", "s5", "s10", "s15", "s20", "s30", # sub minute
  "m1", "m2", "m5", "m10", "m15", "m30", # sub hour
  "h1", "h2", "h4", "h6", "h8", "h12", # sub day
  "D1", "D2", "D3", # sub week
  "W1", "M1", "Y1"] # sub year

MONTH_SECONDS = round(2.592e+6)
YEAR_SECONDS = round(3.154e+7)

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
  "Y1": YEAR_SECONDS,
}

INTERVAL_TO_SQL: dict[str, str] = {
  "s1": "1 seconds",
  "s2": "2 seconds",
  "s5": "5 seconds",
  "s10": "10 seconds",
  "s15": "15 seconds",
  "s20": "20 seconds",
  "s30": "30 seconds",
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

def ago(tz=UTC, **kwargs) -> datetime:
  return datetime.now(tz) - relativedelta(**kwargs)

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

def floor_date(interval: str|int|float, date: datetime=None) -> datetime:
  if isinstance(interval, str):
    interval = interval_to_seconds(interval)
  date = date or datetime.now(UTC)
  epoch_sec = int(date.timestamp())
  floored_epoch = epoch_sec - (epoch_sec % interval)
  floored_datetime = datetime.fromtimestamp(floored_epoch, date.tzinfo)
  return floored_datetime

def ceil_date(secs: int, date: datetime=None) -> datetime:
  return floor_date(secs, date) + timedelta(seconds=secs)

def secs_to_ceil_date(secs: int, date: datetime=None, offset=0) -> datetime:
  date = date or datetime.now(UTC)
  return max(round((ceil_date(secs, date) - (date or datetime.now(UTC))).seconds) + offset, 0)

def floor_utc(interval="m1") -> datetime:
  return floor_date(interval, datetime.now(UTC))

def shift_date(timeframe, date: datetime=None, backwards=False):
  date = date or datetime.now(UTC)
  return date + interval_to_delta(timeframe, backwards)

def fit_interval(from_date: datetime, to_date: datetime=None, target_epochs=100) -> Interval:
  to_date = to_date or datetime.now(UTC)
  diff_seconds = (to_date - from_date).total_seconds()

  if target_epochs > 0:  # Check for valid target_epochs
    target_interval_seconds = diff_seconds / target_epochs
    for interval, seconds in SEC_BY_TF.items():
      if seconds >= target_interval_seconds:  # Find the smallest interval that fits
        return interval

  return "h6"

def round_interval(seconds: float, margin: float = 0.25) -> Interval:
  for interval in SEC_BY_TF:
    if SEC_BY_TF[interval] >= seconds * (1 - margin):
      return interval
  return "h1" # Default to hourly if no match
