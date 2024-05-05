from datetime import datetime, now
from ..model import Resource, Interval, TsdbAdapter


def load_series(db: TsdbAdapter, r: Resource, from_date: datetime, to_date: datetime=now(), interval: Interval="H1") -> list:
    return db.fetch_series(r.name, from_date, to_date, interval)
