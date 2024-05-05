from datetime import datetime, now
from ..model import Resource, Interval, TsdbAdapter


def store(db: TsdbAdapter, r: Resource, table="") -> list:
    return db.insert(r, table)

def store_batch(db: TsdbAdapter, resources: list[Resource], from_date: datetime, to_date: datetime=now(), interval: Interval="H1") -> dict:
    return {r.name: store(db, r, from_date, to_date, interval) for r in resources}
