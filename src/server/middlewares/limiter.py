from asyncio import create_task
from functools import wraps
from typing import Optional
from fastapi import FastAPI, Request, HTTPException, Response
from starlette.middleware.base import BaseHTTPMiddleware

import src.state as state
from src.state import redis
from src.utils import secs_to_ceil_date
from src.cache import NS as REDIS_NS

# TODO: implement pattern/geo tracking to flag rotating proxies
def requester_id(req: Request) -> str:
  return req.client.host

class Limiter(BaseHTTPMiddleware):
  def __init__(
    self,
    app: FastAPI,
    whitelist: list[str] = [],
    blacklist: list[str] = [],
    rpm: int = -1, rph: int = -1, rpd: int = -1, # request per minute/hour/day -1 == limitless
    spm: int = -1, sph: int = -1, spd: int = -1, # total size per minute/hour/day -1 == limitless
    ppm: int = -1, pph: int = -1, ppd: int = -1, # points per minute/hour/day -1 == limitless
    ppr={}
  ):
    super().__init__(app)
    state.server.limiter = self
    self.limits = {
      'rpm': (rpm, 60), 'rph': (rph, 3600), 'rpd': (rpd, 86400),
      'spm': (spm, 60), 'sph': (sph, 3600), 'spd': (spd, 86400),
      'ppm': (ppm, 60), 'pph': (pph, 3600), 'ppd': (ppd, 86400),
    }
    self.whitelist = whitelist
    self.blacklist = blacklist
    self.ppr = ppr

  async def dispatch(self, req: Request, call_next: callable) -> Response:
    user = requester_id(req)
    ppr = self.ppr.get(req.url.path, 1) # default to 1 point per request

    if user in self.blacklist:
      raise HTTPException(status_code=403, detail="Forbidden")

    if user in self.whitelist:
      return await call_next(req) # bypass rate limits

    # Prepare keys and commands for fetching current counts
    keys = []
    for name, (max_val, _) in self.limits.items():
      if max_val is not None:
        key = f"{REDIS_NS}:limiter:{name}:{user}"
        keys.append(key)

    # Add points key if ppr is specified
    if ppr:
      points_key = f"{REDIS_NS}:limiter:points:{user}"
      keys.append(points_key)

    # Fetch current counts
    current_counts = await redis.mget(*keys)
    current_counts = [int(count) if count else 0 for count in current_counts]

    # Check if limits have been breached (pessimistic approach)
    for (name, (max_val, _)), current in zip(self.limits.items(), current_counts):
      if max_val > 0 and current >= max_val:
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded ({name}: {current}/{max_val})")

    # Execute the request and capture the response
    res: Response = await call_next(req)

    # Get the deflated response size
    rsize = int(res.headers.get('Content-Length') or 0) # header injected by starlette
    increments = [1, 1, 1, rsize, rsize, rsize, ppr, ppr, ppr]
    ttls = [
      secs_to_ceil_date(60), # mttl
      secs_to_ceil_date(3600), # httl
      secs_to_ceil_date(86400)] # dttl

    # Prepare commands for incrementing
    limit_pairs, remaining_pairs = [], []
    async with redis.pipeline() as pipe:
      for (name, (max_val, _)), current, increment, ttl in zip(self.limits.items(), current_counts, increments, ttls*3):
        key = f"{REDIS_NS}:limiter:{name}:{user}"
        pipe.incrby(key, increment)
        pipe.expire(key, ttl)
        remaining = max(max_val - (current + increment), 0)
        limit_pairs.append(f"{name}={max_val}")
        remaining_pairs.append(f"{name}={remaining}")

      res.headers.update({
        "X-RateLimit-Limit": ";".join(limit_pairs),
        "X-RateLimit-Remaining": ";".join(remaining_pairs),
        "X-RateLimit-Reset": f"rpm,spm,ppm={ttls[0]};rph,sph,pph={ttls[1]};rpd,spd,ppd={ttls[2]}"
      })
      await pipe.execute()
    return res

# TODO: implement a simpler reflexion mechanism than request->app->middleware
@staticmethod
def limit(points: int):
  def decorator(func: callable):
    @wraps(func)
    async def wrapper(*args, **kwargs):
      req: Request = kwargs.get("req") or args[0]
      limiter: Limiter = state.server.limiter # req.app.limiter
      limiter.ppr.setdefault(req.url.path, points)
      return await func(*args, **kwargs)

    return wrapper

  return decorator
