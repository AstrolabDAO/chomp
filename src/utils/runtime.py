import re
from asyncio import iscoroutinefunction, new_event_loop
from importlib import metadata
from typing import Coroutine, Optional
from importlib import metadata

from src.utils import log_error, log_warn

class PackageMeta:
  def __init__(self, package="chomp"):
    try:
      dist = metadata.distribution(package)
      self.name = dist.metadata["Name"]
      self.version = dist.metadata["Version"]
      self.version_no_patch = ".".join(self.version.split(".")[:2])
      self.description = dist.metadata["Summary"]
      self.authors = dist.metadata.get_all("Author")
    except metadata.PackageNotFoundError:
      print(f"Package {package} not found, using fallback values...")
      self.name = package
      self.version = "0.0.0"
      self.version_no_patch = "0.0"
      self.description = "No description available"
      self.authors = ["Unknown"]

def get_meta(package="chomp"):
  metadata.distribution(package)

def run_async_in_thread(fn: Coroutine):
  loop = new_event_loop()
  try:
    return loop.run_until_complete(fn)
  except Exception as e:
    log_error(f"Failed to run async function in thread: {e}")
    loop.close()

def submit_to_threadpool(executor, fn, *args, **kwargs):
  if iscoroutinefunction(fn):
      return executor.submit(run_async_in_thread, fn(*args, **kwargs))
  return executor.submit(fn, *args, **kwargs)

def select_nested(selector: Optional[str], data: dict) -> any:

  # invalid selectors
  if selector and not isinstance(selector, str):
    log_error("Invalid selector. Please use a valid path string")
    return None

  if not selector or [".", "root"].count(selector.lower()) > 0:
    return data

  # optional starting dot and "root" keyword (case-insensitive)
  if selector.startswith("."):
    selector = selector[1:]

  current = data # base access
  segment_pattern = re.compile(r'([^.\[\]]+)(?:\[(\d+)\])?') # match selector segments eg. ".key" or ".key[index]"

  # loop through segments
  for match in segment_pattern.finditer(selector):
    key, index = match.groups()
    if key.isnumeric() and not index:
      key, index = None, int(key)
    # dict access
    if key and isinstance(current, dict):
      current = current.get(key)
    if not current:
      return log_warn(f"Key not found in dict: {key}")
    # list access
    if index is not None:
      index = int(index)
      if not isinstance(current, list) or index >= len(current):
        return log_error(f"Index out of range in dict.{key}: {index}")
      current = current[index]
  return current
