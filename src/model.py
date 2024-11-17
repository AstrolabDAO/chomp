from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from aiocron import Cron
from typing import Literal, Optional, Type

from src.utils import Interval, TimeUnit, extract_time_unit, interval_to_seconds, split_chain_addr, fmt_date, function_signature

ResourceType = Literal[
  "value", # e.g., inplace document (json/text), binary, int, float, date, string...
  "series", # increment indexed values
  "timeseries" # time indexed values
]

IngesterType = Literal[
  "scrapper", "http_api", "ws_api", "fix_api", # web2
  "evm_caller", "evm_logger", # web3
  "solana_caller", "solana_logger",
  "sui_caller", "sui_logger",
  "aptos_caller", "aptos_logger",
  "ton_caller", "ton_logger",
]

TsdbAdapter = Literal["tdengine", "timescale", "influx", "kdb"]

FieldType = Literal[
  "int8", "uint8", # char, uchar
  "int16", "uint16", # short, ushort
  "int32", "uint32", # int, uint
  "int64", "uint64", # long, ulong
  "float32", "ufloat32", # float, ufloat
  "float64", "ufloat64", # double, udouble
  "bool", "timestamp", "string", "binary", "varbinary"]
HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

@dataclass
class Targettable:
  name: str
  target: str = ""
  selector: str = ""
  method: HttpMethod = "GET"
  headers: dict[str, str] = field(default_factory=dict)
  params: list[any]|dict[str,any] = field(default_factory=list)
  type: FieldType = "float64"
  handler: str = "" # for streams only (json ws, fix...)
  reducer: str = "" # for streams only (json ws, fix...)
  actions: list[any] = field(default_factory=list) # for dynamic scrappers only
  transformers: list[str] = field(default_factory=list)

  @property
  def target_id(self) -> str:
    return md5((self.target + self.selector + str(self.params) + str(self.actions) + function_signature(self.handler)).encode()).hexdigest()

@dataclass
class ResourceField(Targettable):
  transient: bool = False
  value: Optional[any] = None

  def signature(self) -> str:
    return f"{self.name}-{self.type}-{self.target}-{self.selector}-[{','.join(str(self.params))}]-[{','.join(self.transformers) if self.transformers else 'raw'}]"

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  def __hash__(self) -> int:
    return hash(self.id)

  def sql_escape(self) -> str:
    return f"'{self.value}'" if self.type in ["string", "binary", "varbinary"] else str(self.value)

  def chain_addr(self) -> tuple[str|int, str]:
    return split_chain_addr(self.target)

  @classmethod
  def from_dict(cls, d: dict) -> 'ResourceField':
    return cls(**d)

  def to_dict(self) -> dict:
    return {
      "type": self.type,
      "value": self.value, # not relevant in schema
      "transient": self.transient
    }

@dataclass
class Resource:
  name: str
  resource_type: ResourceType = "timeseries"
  fields: list[ResourceField] = field(default_factory=list)
  data_by_field: dict[str, ResourceField] = field(default_factory=dict)

  def to_dict(self) -> dict:
    return {
      # "name": self.name,
      "type": self.resource_type,
      "fields": {field.name: field.type for field in self.fields}
    }

@dataclass
class Ingester(Resource, Targettable):
  interval: Interval = "h1"
  probablity: float = 1.0
  ingester_type: IngesterType = "evm_caller"
  ingestion_time: datetime = None
  cron: Optional[Cron] = None

  @classmethod
  def from_dict(cls, d: dict) -> 'Ingester':
    d["fields"] = [ResourceField.from_dict(field) for field in d["fields"]]
    r = cls(**d)
    for field in r.fields:
      if not field.target: field.target = r.target
      if not field.selector: field.selector = r.selector
      if not field.params: field.params = r.params
      if not field.type: field.type = r.type
      if isinstance(field.params, (list, tuple)):
        for i in range(len(field.params)):
          p = field.params[i]
          if isinstance(p, str) and p.startswith("0x"):
            if len(p) == 42: # ethereum address
              pass # remains str
            if len(p) == 66: # bytes32 (tx hash or else)
              field.params[i] = bytes.fromhex(p[2:])
      else:
        if "time" in field.params: # TODO: genericize to all time fields
          field.params["time"] = round(datetime.now().timestamp())
      if not field.handler: field.handler = r.handler
    return r

  def signature(self) -> str:
    return f"{self.name}-{self.resource_type}-{self.interval}-{self.ingester_type}"\
      + "-".join([field.id for field in self.fields])

  @property
  def id(self) -> str:
    return md5(self.signature().encode()).hexdigest()

  @property
  def interval_sec(self) -> int:
    return interval_to_seconds(self.interval)

  @property
  def precision(self) -> TimeUnit:
    return extract_time_unit(self.interval)

  def __hash__(self) -> int:
    return hash(self.id)

  def values(self):
    return [field.value for field in self.fields]

  def values_dict(self):
    d = {field.name: field.value for field in self.fields if not field.transient}
    d["date"] = self.ingestion_time
    return d

  def load_values(self, values: list[any]):
    for i, field in enumerate(self.fields):
      field.value = values[i]

  def load_values_dict(self, values: dict[str, any]):
    for field in self.fields:
      field.value = values[field.name]

class ConfigMeta(type):
  def __new__(cls, name, bases, dct):
    # iterate over all IngesterType
    for ingester_type in IngesterType.__args__:
      attr_name = ingester_type.lower()
      dct[attr_name] = field(default_factory=list) # add field default to the class
      dct.setdefault('__annotations__', {})[attr_name] = list[Ingester] # add type hint to the class
    return super().__new__(cls, name, bases, dct)

@dataclass
class Config(metaclass=ConfigMeta):
  @classmethod
  def from_dict(cls, data: dict) -> 'Config':
    config_dict = {}
    for ingester_type in IngesterType.__args__:
      key = ingester_type.lower() # match yaml config e.g., scrapper, api, evm
      items = data.get(key, [])
      # inject ingester_type into each to instantiate the correct ingester
      config_dict[key] = [Ingester.from_dict({**item, 'ingester_type': ingester_type}) for item in items]
    return cls(**config_dict)

  @property
  def ingesters(self):
    return self.scrapper + self.http_api + self.ws_api + self.evm_caller + self.evm_logger # + ...

  def to_dict(self) -> dict:
    return { r.name: r.to_dict() for r in self.ingesters }

@dataclass
class Tsdb:
  host: str = "localhost"
  port: int = 6030
  db: str = "default"
  user: str = "rw"
  password: str = "pass"
  conn: any = None
  cursor: any = None

  @classmethod
  async def connect(cls, host: str, port: int, db: str, user: str, password: str):
    raise NotImplementedError
  async def ping(self):
    raise NotImplementedError
  async def ensure_connected(self):
    raise NotImplementedError
  async def close(self):
    raise NotImplementedError
  async def create_db(self, name: str, options: dict, force=False):
    raise NotImplementedError
  async def use_db(self, db: str):
    raise NotImplementedError
  async def create_table(self, c: Ingester, name=""):
    raise NotImplementedError
  async def insert(self, c: Ingester, table=""):
    raise NotImplementedError
  async def insert_many(self, c: Ingester, values: list[tuple], table=""):
    raise NotImplementedError
  async def fetch(self, table: str, from_date: datetime, to_date: datetime, aggregation_interval: Interval, columns: list[str]):
    raise NotImplementedError
  async def fetchall(self):
    raise NotImplementedError
  async def fetch_batch(self, tables: list[str], from_date: datetime, to_date: datetime, aggregation_interval: Interval, columns: list[str]):
    raise NotImplementedError
  async def commit(self):
    raise NotImplementedError
  async def list_tables(self) -> list[str]:
    raise NotImplementedError
