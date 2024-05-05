from typing import Type
from taos import TaosConnection, TaosCursor, TaosResult, TaosStmt, connect, new_bind_params, new_multi_binds
from taos.tmq import Message, Consumer
from ..model import FieldType, Precision, Resource, TsdbAdapter
from os import environ as env

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

PRECISION: Precision = "ms" # ns, us, ms, s, m
TIMEZONE="UTC" # making sure the front-end and back-end are in sync

class TaosAdapter(TsdbAdapter):
  conn: TaosConnection
  cursor: TaosCursor

  def connect(self, host=env["TAOS_HOST"], port=env["TAOS_PORT"], db=env["TAOS_DB"], user=env["DB_RW_USER"], _pass=env["DB_RW_PASS"]):
    self.conn = connect(host=host, port=port, database=db, user=user, password=_pass)
    self.cursor = self.conn.cursor()

  def close(self):
    if self.cursor:
      self.cursor.close()
    if self.conn:
      self.conn.close()

  def get_dbs(self):
    # return self.conn.dbs
    self.cursor.execute("SHOW DATABASES")
    return self.cursor.fetchall()

  def create_db(self, name: str, force=False):
    if not self.conn:
      self.connect()
    base = "CREATE DATABASE IF NOT EXISTS" if force else "CREATE DATABASE"
    self.cursor.execute(f"{base} {name} PRECISION {PRECISION} BUFFER 256 KEEP 3650")

  def use_db(self, db: str):
    if not self.conn:
      self.connect(db=db)
    else:
      self.conn.select_db(db)

  def create_table(self, schema: Resource, name="", force=False):
    if not self.conn:
      self.connect()
    table = name or schema.name
    base = "CREATE TABLE IF NOT EXISTS" if force else "CREATE TABLE"
    fields = ", ".join([f"{field.name} {TYPES[field._type]}" for field in schema.data])
    self.cursor.execute(f"{base} {table} ({fields})")

  def insert(self, data: Resource, table=""):
    table = table or data.name
    fields = ", ".join([field.name for field in data.data])
    values = ", ".join([f"'{field.value}'" for field in data.data])
    self.cursor.execute(f"INSERT INTO {table} ({fields}) VALUES ({values})")

  def insert_many(self, els: list[Resource], table=""):
    if not self.conn:
      self.connect()
    table = table or els[0].name
    fields = ", ".join([field.name for field in els[0].data])
    field_count = len(fields)
    stmt = self.conn.statement("insert into log values(?" + ",?" * (field_count - 1) + ")")
    params = new_multi_binds(field_count)
    for i in fields:
      params[i][PREPARE_STMT[els[0].data[i]._type]]([field.data[i].value for field in els])
    stmt.bind(params)
    stmt.execute()

  def fetch(self, table: str, query: str):
    self.cursor.execute(query)
    return self.cursor.fetchall()

  def commit(self):
    self.conn.commit()
