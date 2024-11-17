import re
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from dateutil.relativedelta import relativedelta
from utils.date import fit_interval

from src.utils import fmt_date, ago, secs_to_ceil_date, parse_date, GENERIC_SPLITTER
import src.state as state
from src.state import redis, tsdb
from src.server.middlewares.limiter import Limiter, limit, requester_id
import src.cache as cache
from src.cache import NS as REDIS_NS
from src.server.responses import JSONResponse

UTC = timezone.utc

router = APIRouter()

@router.get("/")
async def get_root():
  return HTMLResponse("""
<!DOCTYPE html>
  <html style='font-family: monospace;'>
    <div>
    Chomp server is up and running 🤤
    </div>
    <a href="https://github.com/AstrolabDAO/chomp/blob/main/README.md">See docs</a> /
    <a href="/schema">Explore Resources (schema)</a>
  </html>
""")

@router.get("/resources")
@router.get("/schema")
@limit(points=1)
async def get_streams(req: Request):
  return JSONResponse(await cache.get_resource_status())

def parse_resources(resources: str) -> list[str]:
  resources = [r for r in re.split(GENERIC_SPLITTER, resources) if r not in ["", None]]
  if not resources:
    raise HTTPException(status_code=400, detail="No resources provided")
  return resources

@router.get("/last/{resources:path}")
@router.get("/last")
@limit(points=1)
async def get_last(req: Request, resources: str):
  resources = parse_resources(resources)
  res = await cache.get_cache_batch(resources, pickled=True) if len(resources) > 1 else \
    { resources[0]: await cache.get_cache(resources[0], pickled=True) }
  missing_resources = [resource for resource, value in res.items() if value is None]
  if missing_resources:
    raise HTTPException(status_code=404, detail=f"Resources not found: {', '.join(missing_resources)}")
  return JSONResponse(res if len(resources) > 1 else res[resources[0]])

async def get_range(resources: list[str], from_date: str|int, to_date: str|int) -> dict[str, list[dict]]:
  from_date, to_date = parse_date(from_date), parse_date(to_date)
  res = await tsdb.fetch_batch(resources, from_date=from_date, to_date=to_date)
  return res

@router.get("/history/{resources:path}")
@router.get("/history")
@limit(points=10)
async def get_history(req: Request, resources: str, from_date=None, to_date=None):
  resources = parse_resources(resources)
  from_date, to_date = parse_date(from_date or ago(months=1)), parse_date(to_date or datetime.now(UTC))
  interval = fit_interval(from_date, to_date, target_epochs=200)
  return JSONResponse(await tsdb.fetch_batch(tables=resources, from_date=from_date, to_date=to_date, aggregation_interval=interval))

@router.get("/limits")
@limit(points=5)
async def get_limits(req: Request):
  limiter: Limiter = state.server.limiter
  user = requester_id(req)

  # Conciser key fetching with f-strings and list comprehension
  user_limit_keys = [f"{REDIS_NS}:limiter:{name}:{user}" for name in limiter.limits]

  theoretical_ttl_by_interval = { i: secs_to_ceil_date(i) for i in [60, 3600, 86400] }

  # Combine Redis operations in a single pipeline
  async with redis.pipeline(transaction=False) as pipe:
    for key in user_limit_keys:
      pipe.get(key)
      pipe.ttl(key)
    res = await pipe.execute()
    values, ttls = [int(v if v else 0) for v in res[::2]], [int(ttl if ttl > 0 else 0) for ttl in res[1::2]] # unpack by slicing

  limits = {}
  for key, (name, (max_val, interval)), current, ttl in zip(
    user_limit_keys, limiter.limits.items(), values, ttls
  ):
    ttl = ttl or theoretical_ttl_by_interval[interval]
    reset_time_str = fmt_date(datetime.now(UTC) + timedelta(seconds=ttl)) if ttl > 0 else None
    remaining = max(max_val - int(current or 0), 0)  # Handle potentially missing counts

    limits[name] = {
      "cap": max_val,
      "remaining": remaining,
      "ttl": interval,
      "reset": reset_time_str
    }

  return JSONResponse({"user": user, "limits": limits})
