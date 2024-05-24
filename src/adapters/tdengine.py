# TODO: finish+test this adapter

from datetime import datetime
from typing import Optional, Type
from os import environ as env
from taos import TaosConnection, TaosCursor, TaosResult, TaosStmt, connect, new_bind_params, new_multi_binds
from taos.tmq import Message, Consumer

from src.utils import interval_to_sql, log_error, log_info, log_warn
from src.model import Collector, FieldType, Interval, TimeUnit, Resource, Tsdb

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
        else:
          log_error(f"Failed to connect to TDengine on {self.user}@{self.host}:{self.port}/{self.db}", e)
          raise e
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

  async def create_table(self, c: Collector, name=""):
    table = name or c.name
    log_info(f"Creating table {self.db}.{table}...")
    fields = ", ".join([f"`{field.name}` {TYPES[field.type]}" for field in c.data])
    sql = f"""
    CREATE TABLE IF NOT EXISTS {self.db}.{table} (
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

  async def insert(self, c: Collector, table=""):
    await self.ensure_connected()
    table = table or c.name
    persistent_data = [field for field in c.data if not field.transient]
    fields = "`, `".join(field.name for field in persistent_data)
    values = ", ".join([field.sql_escape() for field in persistent_data])
    sql = f"INSERT INTO {self.db}.{table} (ts, `{fields}`) VALUES ('{c.collection_time}', {values});"
    try:
      self.cursor.execute(sql)
    except Exception as e:
      if "Table does not exist" in str(e):
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        self.cursor.execute(sql)
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  async def insert_many(self, c: Collector, values: list[tuple], table=""):
    table = table or c.name
    persistent_fields = [field.name for field in c.data if not field.transient]
    fields = "`, `".join(persistent_fields)
    field_count = len(persistent_fields)
    stmt = self.conn.statement(f"INSER INTO {self.db}.{table} VALUES(?" + ",?" * (field_count - 1) + ")")
    params = new_multi_binds(field_count)
    types = [PREPARE_STMT[field.type] for field in c.data]
    for i in fields:
      params[i][types[i]]([v[i] for v in values])
    stmt.bind(params)
    stmt.execute()

  async def fetch(self, table: str, from_date: Optional[datetime], to_date: Optional[datetime], aggregation_interval: Optional[str], columns: list[str] = []):
    if not to_date:
      to_date = datetime.now()

    agg_bucket = interval_to_sql(aggregation_interval)

    # Define columns with last aggregation
    select_cols = []
    if not columns:
      select_cols = ["last(*) AS *"]  # Select all columns with last aggregation
    else:
      for col in columns:
        select_cols.append(f"last({col}) AS {col}")

    conditions = []
    params = []

    if from_date:
      conditions.append(f"ts >= '{from_date.strftime('%Y-%m-%d %H:%M:%S')}'")
    if to_date:
      conditions.append(f"ts <= '{to_date.strftime('%Y-%m-%d %H:%M:%S')}'")

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"""
      SELECT {select_cols}
      FROM {table}
      {where_clause}
      INTERVAL({agg_bucket}) SLIDING({agg_bucket});
    """
    # GROUP BY tb DESC
    # LIMIT 1;
    return await self.fetch(query)

  async def commit(self):
    self.conn.commit()
