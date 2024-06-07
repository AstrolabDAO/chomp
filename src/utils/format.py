from datetime import UTC, datetime
from dateutil import parser
from hashlib import md5, sha256
import logging
from os import urandom, environ as env
from typing import Literal
from wcwidth import wcswidth
from web3 import Web3

from src.utils.types import is_float

DATETIME_FMT = "%Y-%m-%d %H:%M:%S"
DATETIME_FMT_TZ = f"{DATETIME_FMT} %Z"
DATETIME_FMT_ISO = "%Y-%m-%dT%H:%M:%S%z" # complies with RFC 3339 and ISO 8601
GENERIC_SPLITTER = r"[-/,;|&]"
LOGFILE = env.get("LOGFILE", "out.log")
LogLevel = Literal["INFO", "ERROR", "DEBUG", "WARN"]

def log(level: LogLevel="INFO", *args):
  body = ' '.join(str(arg) for arg in args)
  msg = f"[{fmt_date(datetime.now(UTC))}] {level}: {body}"
  print(msg)
  with open(LOGFILE, "a+") as log:
    log.write(msg + "\n")

def log_debug(*args): log("DEBUG", *args); return True
def log_info(*args): log("INFO", *args); return True
def log_error(*args): log("ERROR", *args); return False
def log_warn(*args): log("WARN", *args); return False

def fmt_date(date: datetime, iso=True, keepTz=True):
  return date.strftime(DATETIME_FMT_ISO if iso else DATETIME_FMT_TZ if keepTz else DATETIME_FMT)

def parse_date(date: str|int):
  if isinstance(date, datetime):
    return date
  try:
    if is_float(date):
      return datetime.fromtimestamp(rebase_epoch_to_sec(float(date)), tz=UTC)
    parsed = parser.parse(date, fuzzy_with_tokens=True, ignoretz=False)[0]
    if not parsed.tzinfo:
      parsed = parsed.replace(tzinfo=UTC)
    return parsed
  except Exception as e:
    log_error(f"Failed to parse date: {date}", e)
    return None

def rebase_epoch_to_sec(epoch: int|float) -> int:
  while epoch >= 10000000000:
    epoch /= 1000
  while epoch <= 100000000:
    epoch *= 1000
  return int(epoch)

loggingToLevel = {
  logging.DEBUG: "DEBUG",
  logging.INFO: "INFO",
  logging.WARNING: "WARN",
  logging.ERROR: "ERROR",
  logging.CRITICAL: "ERROR"
}

class LogHandler(logging.Handler):
  def emit(self, record: logging.LogRecord):
    return log(loggingToLevel[record.levelno], self.format(record))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = [LogHandler()]

def generate_hash(length=32, derive_from="") -> str:
  derive_from = derive_from or datetime.now(UTC).isoformat()
  hash_fn = md5 if length <= 32 else sha256
  return hash_fn((str(derive_from) + urandom(64).hex()).encode()).hexdigest()[:length]

def split_chain_addr(target: str) -> tuple[str|int, str]:
  tokens = target.split(":")
  n = len(tokens)
  if n == 1:
    tokens = ["1", tokens[0]] # default to ethereum L1
  if n > 2:
    raise ValueError(f"Invalid target format for evm: {target}, expected chain_id:address")
  return int(tokens[0]), Web3.to_checksum_address(tokens[1])

def prettify(data, headers):
  col_widths = [max(wcswidth(str(item)) for item in column) for column in zip(headers, *data)]
  row_fmt = "| " + " | ".join(f"{{:<{w}}}" for w in col_widths) + " |"
  x_sep = "+" + "+".join(["-" * (col_width + 2) for col_width in col_widths]) + "+\n"
  return x_sep + row_fmt.format(*headers) + "\n" + x_sep + "\n".join(row_fmt.format(*row) for row in data) + "\n" + x_sep
