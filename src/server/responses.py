from fastapi import Response
from fastapi.exceptions import RequestValidationError, ValidationException, HTTPException, WebSocketException
import orjson

from src.utils.format import generate_hash


class JSONResponse(Response):
  media_type = "application/json"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    self.headers["Content-Type"] += "; charset=utf-8"

  def render(self, content: any) -> bytes:
    return orjson.dumps(content, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_SERIALIZE_DATACLASS)

def error_response(exc: Exception, code=400, message="Bad Request"):
  exc = exc.__dict__
  trace_id = exc.get("trace_id") or generate_hash(16)
  code, message = exc.get("status_code") or code, exc.get("detail") or message
  return JSONResponse(status_code=code, content={"code": code, "message": message, "trace_id": trace_id})

ERROR_HANDLERS = {
  RequestValidationError: lambda request, exc: error_response(exc, code=400, message="Bad Request: validation error"),
  ValidationException: lambda request, exc: error_response(exc, code=400, message="Bad Request: validation error"),
  HTTPException: lambda request, exc: error_response(exc, code=400, message="Bad Request"),
  WebSocketException: lambda request, exc: error_response(exc, code=400, message="Bad Websocket Request"),
}

async def error_handler(request, exc):
  handler = ERROR_HANDLERS.get(
    exc.__class__,
    lambda request, exc: error_response(exc, code=500, message="Server error, try again later and/or report {trace_id} to the team")) # default to 500
  return handler(request, exc)
