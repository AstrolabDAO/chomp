from fastapi import Request
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

import src.state as state

class VersionResolver(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next):
    latest_prefix = f"/v{state.meta.version_no_patch}"  # Cache prefix
    path = request.url.path
    if path.startswith(latest_prefix):
      request.scope["path"] = path.replace(latest_prefix, "", 1) # replace /v{latest} with / (api root)
    response = await call_next(request)
    return response
