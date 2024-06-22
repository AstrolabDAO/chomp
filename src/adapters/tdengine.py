from asyncio import gather
from datetime import UTC, datetime
from os import environ as env
from cache import get_or_set_cache
from taos import TaosConnection, TaosCursor, TaosResult, TaosStmt, connect, new_bind_params, new_multi_binds
from dateutil.relativedelta import relativedelta

from src.utils import log_error, log_info, log_warn, Interval, TimeUnit, interval_to_sql, fmt_date, ago
from src.model import Ingester, FieldType, Resource, Tsdb
import src.state as state


TYPES: dict[FieldType, str] = {
  "int8": "tinyint", # 8 bits char
  "uint8": "tinyint unsigned", # 8 bits uchar
  "int16": "smallint", # 16 bits short
  "uint16": "smallint unsigned", # 16 bits ushort
  "int32": "int", # 32 bits int
  "uint32": "int unsigned", # 32 bits uint
  "int64": "bigint", # 64 bits long
  "uint64": "bigint unsigned", # 64 bits ulong
  "float32": "float", # 32 bits float
  "ufloat32": "float unsigned", # 32 bits ufloat
  "float64": "double", # 64 bits double
  "ufloat64": "double unsigned", # 64 bits udouble
  "bool": "bool", # 8 bits bool
  "timestamp": "timestamp", # 64 bits timestamp, default precision is ms, us and ns are also supported
  "datetime": "timestamp", # 64 bits timestamp
  "string": "nchar", # fixed length array of 4 bytes uchar
  "binary": "binary", # fixed length array of 1 byte uchar
  "varbinary": "varbinary", # variable length array of 1 byte uchar
}

INTERVALS: dict[str, str] = {
  "s1": "1s",
  "s2": "2s",
  "s5": "5s",
  "s10": "10s",
  "s15": "15s",
  "s20": "20s",
  "s30": "30s",
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
  "W1": "1w",
  "M1": "1n",
  "Y1": "1y"
}

PREPARE_STMT = {}
for k, v in TYPES.items():
  PREPARE_STMT[k] = v.replace(" ", "_")

PRECISION: TimeUnit = "ms" # ns, us, ms, s, m
TIMEZONE="UTC" # making sure the front-end and back-end are in sync

class Taos(Tsdb):
  conn: TaosConnection
  cursor: TaosCursor

  @classmethod
  async def connect(
    cls,
    host=env.get("TAOS_HOST", "localhost"),
    port=int(env.get("TAOS_PORT", 6030)),
    db=env.get("TAOS_DB", "default"),
    user=env.get("DB_RW_USER", "rw"),
    password=env.get("DB_RW_PASS", "pass")
  ) -> "Taos":
    self = cls(host, port, db, user, password)
    await self.ensure_connected()
    return self

  async def close(self):
    if self.cursor:
      self.cursor.close()
    if self.conn:
      self.conn.close()

  async def ensure_connected(self):
    if not self.conn:
      try:
        self.conn = connect(
          host=self.host,
          port=self.port,
          database=self.db,
          user=self.user,
          password=self.password
        )
      except Exception as e:
        e = str(e).lower()
        if "database" in e and "not exist" in e:
          self.conn = connect(host=self.host, port=self.port, user=self.user, password=self.password)
          log_warn(f"Database '{self.db}' does not exist on {self.host}:{self.port}, creating it now...")
          await self.create_db(self.db)
        if not self.conn:
          raise ValueError(f"Failed to connect to TDengine on {self.user}@{self.host}:{self.port}/{self.db}")

      log_info(f"Connected to TDengine on {self.host}:{self.port}/{self.db} as {self.user}")
      self.cursor = self.conn.cursor()

    if not self.cursor:
      self.cursor = self.conn.cursor()
    if not self.cursor:
      raise ValueError(f"Failed to connect to TDengine on {self.user}@{self.host}:{self.port}/{self.db}")

  async def get_dbs(self):
    # return self.conn.dbs
    self.cursor.execute("SHOW DATABASES;")
    return self.cursor.fetchall()

  async def create_db(self, name: str, options={}, force=False):
    await self.ensure_connected()
    base = "CREATE DATABASE IF NOT EXISTS" if force else "CREATE DATABASE"
    try:
      self.cursor.execute(f"{base} {name} PRECISION '{PRECISION}' BUFFER 256 KEEP 3650d;") # 10 years max archiving
      log_info(f"Created database {name} with time precision {PRECISION}")
    except Exception as e:
      log_error(f"Failed to create database {name} with time precision {PRECISION}", e)
      raise e

  async def use_db(self, db: str):
    if not self.conn:
      await self.connect(db=db)
    else:
      self.conn.select_db(db)

  async def create_table(self, c: Ingester, name=""):
    table = name or c.name
    log_info(f"Creating table {self.db}.{table}...")
    fields = ", ".join([f"`{field.name}` {TYPES[field.type]}" for field in c.fields if not field.transient])
    sql = f"""
    CREATE TABLE IF NOT EXISTS {self.db}.`{table}` (
      ts timestamp,
      {fields}
    );
    """
    try:
      self.cursor.execute(sql)
      log_info(f"Created table {self.db}.{table}")
    except Exception as e:
      log_error(f"Failed to create table {self.db}.{table}", e)
      raise e

  async def alter_table(self, table: str, add_columns: list[tuple[str, str]] = [], drop_columns: list[str] = []):
    await self.ensure_connected()
    for column_name, column_type in add_columns:
      try:
        self.cursor.execute(f"ALTER TABLE {self.db}.`{table}` ADD COLUMN `{column_name}` {TYPES[column_type]};")
        log_info(f"Added column {column_name} of type {column_type} to {self.db}.{table}")
      except Exception as e:
        log_error(f"Failed to add column {column_name} to {self.db}.{table}", e)
        raise e

    for column_name in drop_columns:
      try:
        self.cursor.execute(f"ALTER TABLE {self.db}.`{table}` DROP COLUMN `{column_name}`;")
        log_info(f"Dropped column {column_name} from {self.db}.{table}")
      except Exception as e:
        log_error(f"Failed to drop column {column_name} from {self.db}.{table}", e)
        raise e

  async def insert(self, c: Ingester, table=""):
    await self.ensure_connected()
    table = table or c.name
    persistent_data = [field for field in c.fields if not field.transient]
    fields = "`, `".join(field.name for field in persistent_data)
    values = ", ".join([field.sql_escape() for field in persistent_data])
    sql = f"INSERT INTO {self.db}.`{table}` (ts, `{fields}`) VALUES ('{c.ingestion_time}', {values});"
    try:
      self.cursor.execute(sql)
    except Exception as e:
      error_message = str(e).lower()
      if "table does not exist" in error_message:
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self.insert(c, table=table)
      elif "invalid column name" in error_message or "statement too long" in error_message:
        log_warn(f"Column mismatch detected, altering table {self.db}.{table} to add missing columns...")
        existing_columns = await self.get_columns(table)
        existing_column_names = [col[0] for col in existing_columns]
        add_columns = [(field.name, field.type) for field in persistent_data if field.name not in existing_column_names]
        await self.alter_table(table, add_columns=add_columns)
        await self.insert(c, table=table)
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  async def insert_many(self, c: Ingester, values: list[tuple], table=""):
    table = table or c.name
    persistent_fields = [field.name for field in c.fields if not field.transient]
    fields = "`, `".join(persistent_fields)
    field_count = len(persistent_fields)
    stmt = self.conn.statement(f"INSER INTO {self.db}.`{table}` VALUES(?" + ",?" * (field_count - 1) + ")")
    params = new_multi_binds(field_count)
    types = [PREPARE_STMT[field.type] for field in c.fields]
    for i in fields:
      params[i][types[i]]([v[i] for v in values])
    stmt.bind(params)
    stmt.execute()

  async def get_columns(self, table: str) -> list[tuple[str, str, str]]:
    try:
      self.cursor.execute(f"DESCRIBE {self.db}.`{table}`;")
      return self.cursor.fetchall()
    except Exception as e:
      log_error(f"Failed to get columns from {self.db}.{table}", e)
      return []

  async def fetch(self, table: str, from_date: datetime=None, to_date: datetime=None, aggregation_interval: Interval="m5", columns: list[str] = []):

    to_date, from_date = to_date or datetime.now(UTC), from_date or ago(years=1)
    agg_bucket = INTERVALS[aggregation_interval]
    columns = columns or await get_or_set_cache(f"{table}:columns",
      callback=lambda: self.get_columns(table),
      expiry=300, pickled=True) # 5 mins cache

    if not columns:
      log_warn(f"No columns found for table {self.db}.{table}")
      return []

    select_cols = ", ".join([f"last(`{col[0]}`) AS {col[0]}" for col in columns])
    conditions = [
      f"ts >= '{fmt_date(from_date, keepTz=False)}'" if from_date else None,
      f"ts <= '{fmt_date(to_date, keepTz=False)}'" if to_date else None,
    ]
    where_clause = f"WHERE {' AND '.join(filter(None, conditions))}" if any(conditions) else ""
    sql = f"SELECT {select_cols} FROM {self.db}.`{table}` {where_clause} INTERVAL({agg_bucket}) SLIDING({agg_bucket}) FILL(prev);" # ORDER BY ts DESC LIMIT 1

    try:
      self.cursor.execute(sql)
      return self.cursor.fetchall()
    except Exception as e:
      log_error(f"Failed to fetch data into {self.db}.{table}", e)
      raise e

  async def fetch_batch(self, tables: list[str], from_date: datetime=None, to_date: datetime=None, aggregation_interval: Interval="m5", columns: list[str] = []):
    to_date = to_date or datetime.now(UTC)
    from_date = from_date or to_date - relativedelta(years=10)
    return await gather(*[self.fetch(table, from_date, to_date, aggregation_interval, columns) for table in tables])

  async def fetch_all(self, query: str) -> TaosResult:
    self.cursor.execute(query)
    return self.cursor.fetchall()

  async def commit(self):
    self.conn.commit()
