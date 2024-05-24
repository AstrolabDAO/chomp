# TODO: finish+test this adapter

from os import environ as env
from datetime import datetime
from typing import Optional
from opentsdb.client import OpenTSDBClient
from opentsdb.query import Query

from src.model import Collector, Interval

INTERVAL_TO_OPENTSDB: dict[Interval, str] = {
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
  "W1": "7w",
  "M1": "4w",
  "Y1": "1y"
}

def interval_to_opentsdb(interval: Optional[str]) -> str:
  return INTERVAL_TO_OPENTSDB.get(interval, None)

class OpenTSDB:
  client: OpenTSDBClient

  @classmethod
  async def connect(cls,
    host=env.get("OPENTSDB_HOST", "localhost"),
    port=int(env.get("OPENTSDB_PORT", 4242)),
    user=env.get("DB_RW_USER", "rw"),
    password=env.get("DB_RW_PASS", "pass")
  ) -> "OpenTSDB":
    self = cls(host, port, user, password)
    await self.ensure_connected()
    return self

  async def close(self):
    self.client.close()

  async def ensure_connected(self):
    if not self.client:
      self.client = OpenTSDBClient(self.host, self.port)
    if not self.client:
      raise ValueError(f"Failed to connect to OpenTSDB on {self.host}:{self.port}")

  async def create_db(self, name: str, options={}, force=False):
    pass # No-op in OpenTSDB

  async def use_db(self, name: str):
    pass # No-op in OpenTSDB

  async def create_table(self, c: Collector, name=""):
    pass # No-op in OpenTSDB

  async def insert(self, c: Collector, table=""):
    table = table or c.name
    data_points = []
    persistent_data = [field for field in c.data if not field.transient]
    for field in persistent_data:
      data_points.append({
        "metric": f"{c.name}.{field.name}", # can also use "tags": {"collector": c.name}
        "value": field.value,
        "timestamp": c.collection_time
      })
    self.client.add_data_points(data_points)

  async def insert_many(self, c: Collector, values: list[tuple], table=""):
    data_points = []
    persistent_data = [field for field in c.data if not field.transient]
    for value in values:
      for i, field in enumerate(persistent_data):
        data_point = {
          "metric": f"{c.name}.{field.name}",
          "value": value[i+1],
          "timestamp": value[0]
        }
        data_points.append(data_point)
    self.client.add_data_points(data_points)

  async def fetch(self, table: str, from_date: Optional[datetime], to_date: Optional[datetime], aggregation_interval: Optional[str], columns: list[str] = []):
    if not to_date:
      to_date = datetime.now()

    agg_bucket = interval_to_opentsdb(aggregation_interval)

    query = Query(start=from_date, end=to_date)
    query.add_metric(table)
    if aggregation_interval:
      query.add_aggregator(agg_bucket)
    if columns:
      query.add_select(columns)
    else:
      query.add_select("*")

    result = self.client.query(query)
    return result

  async def commit(self):
    pass # No-op in OpenTSDB
