from dataclasses import dataclass, field
from typing import Literal, Optional, List, Type
from enum import Enum

class CollectorType(Enum):
  VALUE = 1  # e.g., document (json/text), binary, int, float, date, string...
  SERIES = 2  # increment indexed values
  TIMESERIES = 3  # time indexed values

Precision = Literal["ns", "us", "ms", "s", "m"]
FieldType = Literal["int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "ufloat32", "float64", "ufloat64", "bool", "timestamp", "string", "binary", "varbinary"]

@dataclass
class ResourceField:
  name: str
  _type: FieldType
  target: Optional[str] = None
  selector: Optional[str] = None
  parameters: List[str] = field(default_factory=list)  # Uses field(default_factory=list) for mutable defaults
  transformer: Optional[str] = None
  value: Optional[any] = None

@dataclass
class Resource:
  name: str
  data: List[ResourceField] # fields

@dataclass
class Collector(Resource):
  type: CollectorType
  interval: str  # cron expression only (simpler than supporting literal timeframes)
  inplace: bool  # whether the collector should update the resource in place or create a new one

@dataclass
class CollectorConfig:
  scrappers: List[Collector] = field(default_factory=list)
  apis: List[Collector] = field(default_factory=list)
  evms: List[Collector] = field(default_factory=list)

@dataclass
class TsdbAdapter:
  host: str
  port: int
  db: str
  user: str
  _pass: str
  conn: any
  cursor: any

  def connect(self, host: str, port: int, db: str, user: str, _pass: str):
    raise NotImplementedError
  def close(self):
    raise NotImplementedError
  def create_db(self, name: str, options: dict):
    raise NotImplementedError
  def use_db(self, db: str):
    raise NotImplementedError
  def create_table(self, schema: Type, name_override=""):
    raise NotImplementedError
  def insert(self, table: str, data: dict):
    raise NotImplementedError
  def fetch(self, table: str, query: str):
    raise NotImplementedError
  def fetchall(self):
    raise NotImplementedError
  def commit(self):
    raise NotImplementedError
