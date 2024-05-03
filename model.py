from typing import Optional, List
from enum import Enum
from pydantic import BaseModel

class CollectorType(Enum):
  VALUE = 1 # eg. document (json/text), binary, int, float, date, string...
  SERIES = 2 # increment indexed values
  TIMESERIES = 3 # time indexed values

class ResourceField(BaseModel):
  name: str
  type: str
  target: Optional[str] = None
  selector: Optional[str] = None
  parameters: List[str] = []  # It's okay to use a non-Optional here since the default is an empty list
  transformer: Optional[str] = None

class Resource(BaseModel):
  name: str
  data: List[ResourceField]

class Collector(Resource):
  type: CollectorType
  interval: str # cron expression only (simpler than supporting litteral timeframes)
  inplace: bool # whether the collector should update the resource in place or create a new one

class CollectorConfig(BaseModel):
  scrappers: List[Collector]
  apis: List[Collector]
  evms: List[Collector]
