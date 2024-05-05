from dataclasses import dataclass, field
from typing import Literal, Optional, List, Type
from enum import Enum

import numpy as np
from utils import collector_id, field_id

class ResourceType(str, Enum):
  VALUE = 'value'  # e.g., document (json/text), binary, int, float, date, string...
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
  id: str = ""
  name: str
  type: FieldType
  target: Optional[str] = None
  selector: Optional[str] = None
  parameters: List[str|int|float] = [] # use field(default_factory=list) for mutable defaults
  transformers: Optional[list[str]] = None
  value: Optional[any] = None

  @classmethod
  def from_dict(cls, data: dict) -> 'ResourceField':
    data['type'] = FieldType(data['type'])
    f = cls(**data)
    f.id = field_id(f) # hex digest
    return f

@dataclass
class Resource:
  name: str
  resource_type: ResourceType
  data: List[ResourceField] # fields

  @classmethod
  def from_dict(cls, data: dict) -> 'Resource':
    # instantiate fields first
    fields = [ResourceField.from_dict(field) for field in data['data']]
    return cls(
      name=data['name'],
      resource_type=ResourceType(data['resource_type']),
      fields=fields)

@dataclass
class CollectorConfig(Resource):
  id: str = ""
  collector_type: CollectorType
  interval: Interval
  inplace: bool  # whether the collector should update the resource in place or create a new one

  @classmethod
  def from_dict(cls, data: dict) -> 'CollectorConfig':
    c = cls(
      name=data['name'],
      data=[ResourceField.from_dict(field) for field in data['data']],
      resource_type=ResourceType(data['resource_type']),
      collector_type=CollectorType(data['collector_type']),
      interval=Interval(data['interval']),
      inplace=data['inplace']
    )
    c.id = collector_id(c) # hex digest
    return c

@dataclass
class Config:
  scrapper: List[CollectorConfig] = []
  api: List[CollectorConfig] = []
  evm: List[CollectorConfig] = []
  solana: List[CollectorConfig] = []
  cosmos: List[CollectorConfig] = []
  sui: List[CollectorConfig] = []
  ton: List[CollectorConfig] = []

  @classmethod
  def from_dict(cls, data: dict) -> 'Config':
    config_dict = {}
    for collector_type in CollectorType:
      key = collector_type.value.lower() # match yaml config e.g., scrapper, api, evm
      items = data.get(key, [])
      # inject collector_type into each to instantiate the correct collector
      config_dict[key] = [CollectorConfig.from_dict({**item, 'collector_type': collector_type.value}) for item in items]
    return cls(**config_dict)

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
  def insert(self, table: str, r: Resource, ):
    raise NotImplementedError
  def fetch(self, table: str, query: str):
    raise NotImplementedError
  def fetchall(self):
    raise NotImplementedError
  def commit(self):
    raise NotImplementedError
