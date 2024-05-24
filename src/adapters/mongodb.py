from os import environ as env
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from typing import Optional, List
from datetime import datetime, timedelta

from src.utils import log_info
from src.model import Collector, Interval, Tsdb

INTERVAL_TO_MONGO: dict[Interval, str] = {
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

def interval_to_mongo(interval: Optional[str]) -> str:
  return INTERVAL_TO_MONGO.get(interval, None)

class MongoDb(Tsdb):
  client: Optional[MongoClient] = None
  db: Optional[Database] = None
  collection: Optional[Collection] = None

  @classmethod
  async def connect(cls,
    host=env.get("MONGO_HOST", "localhost"),
    port=int(env.get("MONGO_PORT", 27017)),
    db=env.get("MONGO_DB", "default"),
    user=env.get("DB_RW_USER", "rw"),
    password=env.get("DB_RW_PASS", "pass")
  ) -> "MongoDb":
    self = cls(host, port, db, user, password)
    self.client = MongoClient(host, port, username=user, password=password)
    self.db = self.client[db]
    if not self.client or not self.db:
      raise ValueError(f"Failed to connect to MongoDB on {user}@{host}:{port}/{db}")
    return self

  async def close(self):
    if self.client:
      self.client.close()

  async def ensure_connected(self):
    if not self.client:
      self.client = MongoClient(self.host, self.port, username=self.user, password=self.password)

  async def get_db(self):
    return self.client[self.db]

  async def create_db(self, name: str, options=None, force=False):
    if name not in self.get_db().list_collection_names():
      self.get_db().create_collection(name)
      log_info(f"Created collection {name}")

  async def use_db(self, db_name: str):
    self.db = db_name

  async def create_table(self, c: Collector, name: str = "", force: bool = False):
    table = name or c.name
    self.collection = self.get_db()[table]
    if table not in self.get_db().list_collection_names():
      self.collection.create_index([("ts", 1)], unique=True)
      log_info(f"Created timeseries table {self.get_db().name}.{table}")

  async def insert(self, c: Collector, table: str = ""):
    table = table or c.name
    self.collection = self.get_db()[table]
    data = {"_id": c.collection_time}
    persistent_data = {field.name: field.value for field in c.data if not field.transient}
    for field in persistent_data:
      data[field.name] = field.value
    self.collection.insert_one(data)

  async def insert_many(self, c: Collector, values: List[tuple], table: str = ""):
    table = table or c.name
    self.collection = self.get_db()[table]
    data = [{"_id": value[0], **{field.name: value[i + 1] for i, field in enumerate(c.data) if not field.transient}} for value in values]
    self.collection.insert_many(data)

  async def fetch(self, table: str, from_date: Optional[datetime], to_date: Optional[datetime], aggregation_interval: Optional[Interval], columns: List[str] = []):
    if not to_date:
      to_date = datetime.now()

    query = {"_id": {"$gte": from_date, "$lte": to_date}}
    projection = {"_id": 0}
    if columns:
      projection.update({col: 1 for col in columns})

    pipeline = [{"$match": query}, {"$project": projection}]
    if aggregation_interval:
      pipeline.append({"$bucketAuto": {"groupBy": "$_id", "buckets": interval_to_mongo(aggregation_interval)}})
    cursor = self.collection.aggregate(pipeline)
    return list(cursor)

  async def fetchall(self, table: str):
    return await self.fetch(table)

  async def commit(self):
    pass  # No-op in MongoDB
