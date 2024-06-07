from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager
from starlette.middleware.gzip import GZipMiddleware
import uvicorn

import src.state as state
from .responses import error_handler
from .routers import forwarder, retriever
from .middlewares import Limiter, VersionResolver

@asynccontextmanager
async def lifespan(app: FastAPI):
  # pre-startup
  yield
  # post-shutdown

# TODO: these should be configurable as well as endpoints ppr
DEFAULT_LIMITS = {
  'rpm': 60, 'rph': 1200, 'rpd': 9600, # 1/.3/.1 logarithmic req caps (burst protection)
  'spm': 1e8, 'sph': 2e9, 'spd': 1.6e10, # 1/.3/.1 logarithmic bandwidth caps (burst protection)
  'ppm': 60, 'pph': 1200, 'ppd': 9600, # 1/.3/.1 logarithmic points caps (burst protection)
}

async def start():
  app = FastAPI(lifespan=lifespan, exception_handlers={ Exception: error_handler })
  state.server = app
  for base in ["", f"/v{state.meta.version}", f"/v{state.meta.version_no_patch}"]: # /v{latest} aliasing to /
    app.include_router(retriever.router, prefix=base) # http_api
    app.include_router(forwarder.router, prefix=base) # ws_api

  # app.add_middleware(VersionResolver) # redirects /v{latest} to /
  app.add_middleware(Limiter, **DEFAULT_LIMITS) # rate limiting (req count/bandwidth/points)
  app.add_middleware(GZipMiddleware, minimum_size=1e3) # only compress responses > 1kb

  config = uvicorn.Config(
    app,
    host=state.args.host,
    port=state.args.port,
    ws_ping_interval=state.args.ws_ping_interval,
    ws_ping_timeout=state.args.ws_ping_timeout,
    log_config=None) # defaults to using utils.logger

  server = uvicorn.Server(config)
  await server.serve()
