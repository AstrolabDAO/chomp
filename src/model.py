from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from aiocron import Cron
from typing import Literal, Optional, Type

from utils import interval_to_seconds
from web3 import Web3

ResourceType = Literal[
  "value", # e.g., inplace document (json/text), binary, int, float, date, string...
  "series", # increment indexed values
  "timeseries" # time indexed values
]

CollectorType = Literal[
  "scrapper", "http_api", "ws_api", "fix_api", # web2
  "evm", "cosmos", "solana", "sui", "ton" # web3
]

TsdbAdapter = Literal["tdengine", "timescale", "influx", "kdb"]

# below are based on ISO 8601 capitalization (cf. https://en.wikipedia.org/wiki/ISO_8601)
TimeUnit = Literal["ns", "us", "ms", "s", "m", "h", "D", "W", "M", "Y"]
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
class Targettable:
  name: str
  target: str = ""
  selector: str = ""
  params: list[str|int|float] = field(default_factory=list)
  handler: str = "" # for streams only (json ws, fix...)
  reducer: str = "" # for streams only (json ws, fix...)
  transformers: list[str] = field(default_factory=list)

@dataclass
class ResourceField(Targettable):
  type: FieldType = "float64"
  value: Optional[any] = None

  @classmethod
  def from_dict(cls, d: dict) -> 'ResourceField':
    return cls(**d)

  def signature(self) -> str:
    return f"{self.name}-{self.type}-{self.target}-{self.selector}-[{','.join(self.params)}]-[{','.join(self.transformers) if self.transformers else 'raw'}]"

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  def chain_addr(self) -> tuple[str|int, str]:
    tokens = self.target.split(":")
    n = len(tokens)
    if n == 1:
      tokens = ["1", tokens[0]] # default to ethereum L1
    if n > 2:
      raise ValueError(f"Invalid target format for evm: {self.target}, expected chain_id:address")
    return [int(tokens[0]), Web3.to_checksum_address(tokens[1])]

  def __hash__(self) -> int:
    return hash(self.id)

@dataclass
class Resource:
  name: str
  resource_type: ResourceType = "timeseries"
  data: list[ResourceField] = field(default_factory=list)
  data_by_field: dict[str, ResourceField] = field(default_factory=dict)

@dataclass
class Collector(Resource, Targettable):
  interval: Interval = "h1"
  collector_type: CollectorType = "evm"
  collection_time: datetime = None
  cron: Optional[Cron] = None

  @classmethod
  def from_dict(cls, d: dict) -> 'Collector':
    d["data"] = [ResourceField.from_dict(field) for field in d["data"]]
    r = cls(**d)
    for field in r.data:
      if not field.target: field.target = r.target
      if not field.selector: field.selector = r.selector
      if not field.handler: field.handler = r.handler
    return r

  def signature(self) -> str:
    return f"{self.name}-{self.resource_type}-{self.interval}-{self.collector_type}"\
      + "-".join([field.id for field in self.data])

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  @property
  def interval_sec(self) -> int:
    return interval_to_seconds(self.interval)

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
  scrapper: list[Collector] = field(default_factory=list)
  http_api: list[Collector] = field(default_factory=list)
  ws_api: list[Collector] = field(default_factory=list)
  fix_api: list[Collector] = field(default_factory=list)
  evm: list[Collector] = field(default_factory=list)
  solana: list[Collector] = field(default_factory=list)
  cosmos: list[Collector] = field(default_factory=list)
  sui: list[Collector] = field(default_factory=list)
  ton: list[Collector] = field(default_factory=list)

  @classmethod
  def from_dict(cls, data: dict) -> 'Config':
    config_dict = {}
    for collector_type in CollectorType.__args__:
      key = collector_type.lower() # match yaml config e.g., scrapper, api, evm
      items = data.get(key, [])
      # inject collector_type into each to instantiate the correct collector
      config_dict[key] = [Collector.from_dict({**item, 'collector_type': collector_type}) for item in items]
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
  async def connect(cls, host: str, port: int, db: str, user: str, password: str):
    raise NotImplementedError
  async def close(self):
    raise NotImplementedError
  async def create_db(self, name: str, options: dict):
    raise NotImplementedError
  async def use_db(self, db: str):
    raise NotImplementedError
  async def create_table(self, schema: Type, name_override=""):
    raise NotImplementedError
  async def insert(self, c: Collector, table=""):
    raise NotImplementedError
  async def insert_many(self, c: Collector, values: list[tuple], table=""):
    raise NotImplementedError
  async def fetch(self, table: str, query: str):
    raise NotImplementedError
  async def fetchall(self):
    raise NotImplementedError
  async def commit(self):
    raise NotImplementedError
