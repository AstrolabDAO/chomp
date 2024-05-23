# TODO: finish+test this adapter

from os import environ as env
from datetime import datetime
from typing import Optional
from pykx import Kdb as _Kdb, KdbException

from src.utils import log_info, log_error, log_warn
from src.model import Collector, FieldType, Tsdb, TimeUnit

# Define the data types mapping
TYPES: dict[FieldType, str] = {
  "int8": "short", # 16 bits minimum, no unsigned
  "uint8": "short",
  "int16": "short",
  "uint16": "int",
  "int32": "int",
  "uint32": "long",
  "int64": "long",
  "uint64": "float",
  "float32": "float",
  "ufloat32": "float",
  "float64": "float",
  "ufloat64": "float",
  "bool": "boolean",
  "timestamp": "timestamp", # Adjust based on kdb version documentation
  "datetime": "timestamp", # Adjust based on kdb version documentation
  "string": "symbol",
  "binary": "byte",
  "varbinary": "byte",
}

INTERVAL_TO_Q: dict[str, str] = {
  "m1": "1m",
  "m2": "2m",
  "m5": "5m",
  "m10": "10m",
  "m15": "15m",
  "m30": "30m",
  "h1": "1h",
  "h2": "2h",
  "h4": "4h",
  "h6": "6h",
  "h8": "8h",
  "h12": "12h",
  "D1": "1d",
  "D2": "2d",
  "D3": "3d",
  "W1": "7d",
  "M1": "1M",
  "Y1": "1y",
}

def interval_to_q(interval: Optional[str]) -> str:
  return INTERVAL_TO_Q.get(interval, None)

class Kdb(Tsdb):
  conn: _Kdb = None

  @classmethod
  async def connect(cls,
    host=env.get("KDB_HOST", "localhost"),
    port=int(env.get("KDB_PORT", 5000)),
    user=env.get("DB_RW_USER", "rw"),
    password=env.get("DB_RW_PASS", "pass")
  ) -> "Kdb":
    self = cls(**locals())
    self.ensure_connected()
    return self

  async def close(self):
    if self.conn:
      self.conn.close()

  async def ensure_connected(self):
    if not self.conn:
      self.conn = _Kdb(self.host, self.port, self.user, self.password)
    if not self.conn:
      raise ValueError(f"Failed to connect to Kdb on {self.user}@{self.host}:{self.port}")

  async def create_db(self, name: str, options={}, force=False):
    log_warn("Kdb does not support creating databases. Please make sure the database exists.")

  async def use_db(self, db: str):
    log_warn("Kdb does not support switching databases.")

  async def create_table(self, c: Collector, name: str = "", force: bool = False):
    table = name or c.name
    fields = ", ".join([f"{field.name} {TYPES[field.type]}" for field in c.data])
    # Consider using kdb specific timestamp type based on documentation
    sql = f"{table}:([]ts:timestamp$(); {fields})"
    try:
      self.conn.ks(sql)
      log_info(f"Created table {table}")
    except KdbException as e:
      log_error(f"Failed to create table {table}", e)
      raise e

  async def insert(self, c: Collector, table: str = ""):
    table = table or c.name
    fields = ", ".join([field.name for field in c.data])
    values = ", ".join([f"{field.value}" for field in c.data])
    insert_query = f"{table}.insert((.z.p; {values}))"
    try:
      self.conn.ks(insert_query)
    except KdbException as e:
      log_error(f"Failed to insert data into {table}", e)
      raise e

  async def insert_many(self, c: Collector, values: list[tuple], table: str = ""):
    table = table or c.name
    fields = ", ".join([field.name for field in c.data])
    values_str = ", ".join([f"({', '.join(str(v) for v in value)})" for value in values])
    insert_query = f"{table}.insert(({', '.join(['.z.p'] + [field.name for field in c.data])}) each {values_str})"
    try:
      self.conn.ks(insert_query)
    except KdbException as e:
      log_error(f"Failed to insert data into {table}", e)
      raise e

  async def fetch(self, table: str, from_date: Optional[datetime], to_date: Optional[datetime], aggregation_interval: Optional[str], columns: list[str] = []):
    if not to_date:
        to_date = datetime.now()

    agg_bucket = interval_to_q(aggregation_interval)
    select_cols = columns if columns else ["ts", "*"]

    conditions = []
    if from_date:
        conditions.append(f"ts >= {from_date}")
    if to_date:
        conditions.append(f"ts <= {to_date}")

    where_clause = " and ".join(conditions) if conditions else ""

    group_by = f"ms xbar ts"  # default grouping
    if aggregation_interval:
        group_by = f"{agg_bucket} xbar ts"  # use kdb bucket for aggregation

    select_cols_str = ", ".join(select_cols)

    query = f"select {select_cols_str} from {table} where {where_clause} group by {group_by}"
    try:
        result = self.conn.k(query)
        return result
    except KdbException as e:
        log_error(f"Failed to fetch data from {table}", e)
        raise e

  async def fetchall(self, table: str):
    return await self.fetch(table)

  async def commit(self):
    pass # No-op in Kdb
