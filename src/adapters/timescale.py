# TODO: finish+test this adapter

from datetime import datetime
from typing import Optional
import asyncpg
from dataclasses import dataclass
from os import environ as env

from src.utils import interval_to_sql, log_error, log_info, log_warn
from src.model import Collector, FieldType, Interval, Tsdb, TimeUnit

# Define the data types mapping
TYPES: dict[FieldType, str] = {
  "int8": "smallint", # 16 bits minimum, no unsigned
  "uint8": "smallint",
  "int16": "smallint",
  "uint16": "integer",
  "int32": "integer",
  "uint32": "bigint",
  "int64": "bigint",
  "uint64": "numeric",
  "float32": "real",
  "ufloat32": "real",
  "float64": "double precision",
  "ufloat64": "double precision",
  "bool": "boolean",
  "timestamp": "timestamptz",
  "datetime": "timestamptz",
  "string": "text",
  "binary": "bytea",
  "varbinary": "bytea",
}

# Define precision and timezone
PRECISION: TimeUnit = "ms"  # Supported values: ns, us, ms, s, m
TIMEZONE = "UTC"

partitioning_by_precision: dict[TimeUnit, str] = {
  "ns": "1 day",
  "us": "1 day",
  "ms": "1 day",
  "s": "7 days",
  "m": "7 days",
  "h": "1 month",
  "D": "6 months",
  "W": "5 years",
  "M": "10 years",
  "Y": "100 years"
}

@dataclass
class TimescaleDb(Tsdb):
  pool: asyncpg.Pool = None

  @classmethod
  async def connect(
    cls,
    host: str = env.get("TIMESCALE_HOST", "localhost"),
    port: int = int(env.get("TIMESCALE_PORT", 5432)),
    db: str = env.get("TIMESCALE_DB", "default"),
    user: str = env.get("DB_RW_USER", "rw"),
    password: str = env.get("DB_RW_PASS", "pass")
  ) -> "TimescaleDb":
    self = cls(host, port, db, user, password)
    await self.ensure_connected()
    return self

  async def close(self):
    if self.pool:
      self.pool.terminate()

  async def ensure_connected(self):
    if not self.pool:
      try:
        self.pool = await asyncpg.create_pool(
          host=self.host, port=self.port,
          user=self.user, password=self.password,
          database=self.db,
          max_inactive_connection_lifetime=31)
      except asyncpg.InvalidCatalogNameError as e:
        e = str(e).lower()
        if "database" in e and "not exist" in e:
          self.pool = await asyncpg.create_pool(
            host=self.host, port=self.port,
            user=self.user, password=self.password,
            db="postgres", # root only
            max_inactive_connection_lifetime=31)
          await self.create_db(self.db)
        else:
          log_error(f"Failed to connect to TimescaleDB on {self.user}@{self.host}:{self.port}/{self.db}", e)
          raise e
      self.pool = await asyncpg.create_pool(
        host=self.host, port=self.port,
        user=self.user, password=self.password,
        max_inactive_connection_lifetime=31)
      if not self.pool:
        raise ValueError(f"Failed to connect to TimescaleDB on {self.user}@{self.host}:{self.port}/{self.db}")

      log_info(f"Connected to TimescaleDB on {self.user}@{self.host}:{self.port}/{self.db}")

  async def create_db(self, name: str, options={}, force=False):
    await self.ensure_connected()
    try:
      await self.pool.execute(f"CREATE DATABASE {name}")
      await self.pool.execute(f"ALTER DATABASE {name} SET timezone TO '{TIMEZONE}'; SELECT pg_reload_conf();") # root only
      log_info(f"Created database {name}")
    except Exception as e:
      log_error(f"Failed to create database {name}", e)
      raise e

  async def use_db(self, db: str):
    # NB: no USE sql command in postgres unlike tdengine or mysql, the pool has to be re-created
    # \c with psql is a client command, not a SQL command
    if self.pool:
      await self.pool.close()
    self.conn = await asyncpg.create_pool(
      host=self.host, port=self.port, database=db,
      user=self.user, password=self.password
    )

  async def create_table(self, c: Collector, name: str = "", force: bool = False):
    table = name or c.name
    fields = ", ".join([f"{field.name} {TYPES[field.type]}" for field in c.data])
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
      ts TIMESTAMPTZ NOT NULL,
      {fields}
    );
    SELECT create_hypertable('{table}', by_range('ts', INTERVAL '{partitioning_by_precision[c.precision]}'), if not exists => TRUE);
    """
    try:
      await self.pool.execute(sql)
      log_info(f"Created table {self.db}.{table}")
    except Exception as e:
      log_error(f"Failed to create table {self.db}.{table}", e)
      raise e

  async def insert(self, c: Collector, table: str = ""):
    table = table or c.name
    persistent_data = [field for field in c.data if not field.transient]
    fields = ", ".join(field.name for field in persistent_data)
    values_tpl = ", ".join([f"${i+2}" for i in range(len(persistent_data))])
    insert_query = f"INSERT INTO {table} (ts, {fields}) VALUES ($1, {values_tpl})"
    try:
      await self.pool.execute(insert_query, c.collection_time, *[field.value for field in persistent_data])
    except asyncpg.UndefinedTableError as e:
      e = str(e).lower()
      if "relation" in e and "does not exist" in e:
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self.pool.execute(insert_query, c.collection_time, *[field.value for field in persistent_data])
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  async def insert_many(self, c: Collector, values: list[tuple], table: str = ""):
    table = table or c.name
    persistent_fields = [field for field in c.data if not field.transient]
    fields = ", ".join([field.name for field in persistent_fields])
    field_count = len(persistent_fields)
    values_str = ", ".join([f"(${i*field_count+j+2})" for i in range(len(values)) for j in range(field_count)])
    insert_query = f"INSERT INTO {table} (time, {fields}) VALUES {values_str}"
    # flattened_values = [item for sublist in values for item in sublist]
    try:
      await self.pool.executemany(insert_query, [[value[0], *value[1:]] for value in values])
    except asyncpg.UndefinedTableError as e:
      if "relation" in str(e) and "does not exist" in str(e):
        log_warn(f"Table {self.db}.{table} does not exist, creating it now...")
        await self.create_table(c, name=table)
        await self.pool.executemany(insert_query, [[value[0], *value[1:]] for value in values])
      else:
        log_error(f"Failed to insert data into {self.db}.{table}", e)
        raise e

  # TODO: use window functions, continuous aggregates + real time aggregates for max performance
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
      conditions.append("ts >= $1")
      params.append(from_date)
    if to_date:
      conditions.append("ts <= $2")
      params.append(to_date)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    query += f"""
      SELECT {select_cols}
      FROM {table}
      {where_clause}
      GROUP BY time_bucket('{agg_bucket}', ts), ts
      ORDER BY ts DESC;
    """
    # LIMIT 1;
    # Fetch data with asyncpg
    return await self.pool.fetch(query, *params)

  async def fetchall(self, table: str):
    return await self.fetch(table)

  async def commit(self):
    await self.pool.execute("COMMIT")
