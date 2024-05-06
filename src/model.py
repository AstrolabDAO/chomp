from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from typing import Literal, Optional, List, Type
from enum import Enum

import numpy as np

class ResourceType(str, Enum):
  VALUE = 'value'  # e.g., inplace document (json/text), binary, int, float, date, string...
  SERIES = 'series'  # increment indexed values
  TIMESERIES = 'timeseries'  # time indexed values

class CollectorType(str, Enum):
  SCRAPPER = 'scrapper'
  API = 'api'
  EVM = 'evm'
  TON = 'ton'
  SOLANA = 'solana'
  SUI = 'sui'
  COSMOS = 'cosmos'

class TsdbAdapter(str, Enum):
  TDENGINE = 'tdengine'
  TAOS = 'tdengine'
  TIMESCALE = 'timescale'
  INFLUX = 'influx'
  KDB = 'kdb'

# below are based on ISO 8601 capitalization (cf. https://en.wikipedia.org/wiki/ISO_8601)
Precision = Literal["ns", "us", "ms", "s", "m"]
Interval = Literal[
  "m1", "m2", "m5", "m10", "m15", "m30", # sub hour
  "h1", "h2", "h4", "h6", "h8", "h12", # sub day
  "D1", "D2", "D3", # sub week
  "W1", "M1", "Y1"] # sub year
FieldType = Literal[
  "int8", "uint8", # char, uchar
  "int16", "uint16", # short, ushort
  "int32", "uint32", # int, uint
  "int64", "uint64", # long, ulong
  "float32", "ufloat32", # float, ufloat
  "float64", "ufloat64", # double, udouble
  "bool", "timestamp", "string", "binary", "varbinary"]

@dataclass
class ResourceField:
  name: str
  type: FieldType
  target: Optional[str] = None
  selector: Optional[str] = None
  parameters: List[str|int|float] = field(default_factory=list)
  transformers: Optional[list[str]] = None
  value: Optional[any] = None

  @classmethod
  def from_dict(cls, d: dict, default_target="") -> 'ResourceField':
    d["target"] = d.get("target", default_target)
    return cls(**d)

  def signature(self) -> str:
    return f"{self.name}-{self.type}-{self.target}-{self.selector}-{','.join(self.parameters)}-{','.join(self.transformers) if self.transformers else 'raw'}"

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  def __hash__(self) -> int:
    return hash(self.id)

@dataclass
class Resource:
  name: str
  resource_type: ResourceType
  data: List[ResourceField] # fields
  data_by_field: dict[str, ResourceField] = field(default_factory=dict)

  @classmethod
  def from_dict(cls, d: dict) -> 'Resource':
    d["data"] = [ResourceField.from_dict(field) for field in d["data"]]
    d["resource_type"] = ResourceType(d["resource_type"])
    return cls(**d)

@dataclass
class Collector(Resource):
  collector_type: CollectorType = "evm"
  interval: Interval = "h1"
  target: str = ""
  collection_time: datetime = None

  @classmethod
  def from_dict(cls, d: dict) -> 'Collector':
    d["data"] = [ResourceField.from_dict(field, d.get("target", "")) for field in d["data"]]
    d["resource_type"] = ResourceType(d["resource_type"])
    d["collector_type"] = CollectorType(d["collector_type"])
    return cls(**d)

  def signature(self) -> str:
    return f"{self.name}-{self.resource_type}-{self.interval}-{self.collector_type}"\
      + "-".join([field.id for field in self.data])

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  def __hash__(self) -> int:
    return hash(self.id)

  def values(self):
    return [field.value for field in self.data]

  def values_dict(self):
    return {field.name: field.value for field in self.data}

  def load_values(self, values: list[any]):
    for i, field in enumerate(self.data):
      field.value = values[i]

  def load_values_dict(self, values: dict[str, any]):
    for field in self.data:
      field.value = values[field.name]

@dataclass
class Config:
  scrapper: List[Collector] = field(default_factory=list)
  api: List[Collector] = field(default_factory=list)
  evm: List[Collector] = field(default_factory=list)
  solana: List[Collector] = field(default_factory=list)
  cosmos: List[Collector] = field(default_factory=list)
  sui: List[Collector] = field(default_factory=list)
  ton: List[Collector] = field(default_factory=list)

  @classmethod
  def from_dict(cls, data: dict) -> 'Config':
    config_dict = {}
    for collector_type in CollectorType:
      key = collector_type.value.lower() # match yaml config e.g., scrapper, api, evm
      items = data.get(key, [])
      # inject collector_type into each to instantiate the correct collector
      config_dict[key] = [Collector.from_dict({**item, 'collector_type': collector_type.value}) for item in items]
    return cls(**config_dict)

@dataclass
class Tsdb:
  host: str = "localhost"
  port: int = "6030"
  db: str = "default"
  user: str = "rw"
  password: str = "pass"
  conn: any = None
  cursor: any = None

  @classmethod
  def connect(cls, host: str, port: int, db: str, user: str, password: str):
    raise NotImplementedError
  def close(self):
    raise NotImplementedError
  def create_db(self, name: str, options: dict):
    raise NotImplementedError
  def use_db(self, db: str):
    raise NotImplementedError
  def create_table(self, schema: Type, name_override=""):
    raise NotImplementedError
  def insert(self, c: Collector, table=""):
    raise NotImplementedError
  def insert_many(self, c: Collector, values: list[tuple], table=""):
    raise NotImplementedError
  def fetch(self, table: str, query: str):
    raise NotImplementedError
  def fetchall(self):
    raise NotImplementedError
  def commit(self):
    raise NotImplementedError
