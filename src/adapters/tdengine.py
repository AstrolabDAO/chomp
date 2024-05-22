from typing import Type
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
  "timestamp": "timestamp", # 64 bits timestamp
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
    host=env["TAOS_HOST"],
    port=int(env["TAOS_PORT"]),
    db=env["TAOS_DB"],
    user=env["DB_RW_USER"],
    password=env["DB_RW_PASS"]
  ) -> "Taos":
    self = cls(host, port, db, user, password)
    try:
      self.conn = connect(host=host, port=port, database=db, user=user, password=password)
    except Exception as e:
      if "Database not exist" in str(e):
        self.conn = connect(host=host, port=port, user=user, password=password)
        log_warn(f"Database '{db}' does not exist on {host}:{port}, creating it now...")
        await self.create_db(db)
      else:
        log_error(f"Failed to connect to TDengine on {host}:{port}/{db} as {user}", e)
        raise e
    log_info(f"Connected to TDengine on {host}:{port}/{db} as {user}")
    self.cursor = self.conn.cursor()
    return self

  async def close(self):
    if self.cursor:
      self.cursor.close()
    if self.conn:
      self.conn.close()

  async def ensure_connected(self):
    if not self.conn:
      await self.connect()
    if not self.cursor:
      self.cursor = self.conn.cursor()

  async def get_dbs(self):
    # return self.conn.dbs
    self.cursor.execute("SHOW DATABASES;")
    return self.cursor.fetchall()

  async def create_db(self, name: str, force=False):
    await self.ensure_connected()
    base = "CREATE DATABASE IF NOT EXISTS" if force else "CREATE DATABASE"
    try:
      self.cursor.execute(f"{base} {name} PRECISION '{PRECISION}' BUFFER 256 KEEP 3650d;") # 10 years max archiving
    except Exception as e:
      log_error(f"Failed to create database {name} with time precision {PRECISION}", e)
      raise e
    log_info(f"Created database {name} with time precision {PRECISION}")

  async def use_db(self, db: str):
    if not self.conn:
      await self.connect(db=db)
    else:
      self.conn.select_db(db)

  async def create_table(self, c: Collector, name="", force=False):
    await self.ensure_connected()
    table = name or c.name
    log_info(f"Creating table {self.db}.{table}...")
    base = "CREATE TABLE IF NOT EXISTS" if force else "CREATE TABLE"
    fields = ", ".join([f"`{field.name}` {TYPES[field.type]}" for field in c.data])
    sql = f"{base} {self.db}.{table} (ts timestamp, {fields});"
    self.cursor.execute(sql)

  async def insert(self, c: Collector, table=""):
    table = table or c.name
    fields = "`, `".join([field.name for field in c.data])
    values = ", ".join([f"{field.value}" for field in c.data])
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
    await self.ensure_connected()
    table = table or c.name
    fields = "`, `".join([field.name for field in c.data])
    field_count = len(c.data)
    stmt = self.conn.statement(f"INSER INTO {self.db}.{table} VALUES(?" + ",?" * (field_count - 1) + ")")
    params = new_multi_binds(field_count)
    types = [PREPARE_STMT[field.type] for field in c.data]
    for i in fields:
      params[i][types[i]]([v[i] for v in values])
    stmt.bind(params)
    stmt.execute()

  async def fetch(self, query: str):
    self.cursor.execute(query)
    return self.cursor.fetchall()

  async def fetch_series(self, c: Collector, from_date: int, to_date: int, aggregation_interval=None, fields=[]) -> list:
    if not fields or len(fields) == 0:
      fields = ["*"]
    if not aggregation_interval:
      aggregation_interval = c.interval
    sql_tf = interval_to_sql(aggregation_interval)
    # query = f"SELECT {', '.join(fields)} FROM {table} WHERE ts >= {from_date} AND ts <= {to_date} GROUP BY ts DIV {interval} ORDER BY ts"
    query = f"SELECT {', '.join(fields)} FROM {self.db}.{c.name} WHERE ts >= {from_date} AND ts <= {to_date} INTERVAL({sql_tf}) SLIDING({sql_tf});"
    self.cursor.execute(query)
    return self.cursor.fetchall()

  async def commit(self):
    self.conn.commit()
