import re
import string
import json
from hashlib import sha256, md5
import numpy as np
from asyncio import TimeoutError as FutureTimeoutError

import src.state as state
from src.model import Collector, Resource, ResourceField, Tsdb
from src.actions.load import load
from src.safe_eval import safe_eval
from src.utils import interval_to_delta, log_debug, log_error

BASE_TRANSFORMERS: dict[str, callable] = {
  "lower": lambda r, self: str(self).lower(),
  "upper": lambda r, self: str(self).upper(),
  "capitalize": lambda r, self: str(self).capitalize(),
  "title": lambda r, self: str(self).title(),
  "int": lambda r, self: int(self),
  "float": lambda r, self: float(self),
  "str": lambda r, self: str(self),
  "bool": lambda r, self: bool(self),
  "to_json": lambda r, self: json.dumps(self, indent=2),
  "to_snake": lambda r, self: "_".join(self.lower().split(" ")),
  "to_kebab": lambda r, self: "-".join(self.lower().split(" ")),
  "slugify": lambda r, self: "-".join(self.lower().split(" ")),
  "to_camel": lambda r, self: "".join([i.capitalize() for i in self.split(" ")]),
  "to_pascal": lambda r, self: "".join([i.capitalize() for i in self.split(" ")]),
  "strip": lambda r, self: str(self).strip(),
  "shorten_address": lambda r, self: f"{self[:6]}...{self[-4:]}", # shorten evm address
  "remove_punctuation": lambda r, self: self.translate(str.maketrans('', '', string.punctuation)),
  "reverse": lambda r, self: str(self)[::-1],
  "bin": lambda r, self: bin(int(self))[2:], # int to binary
  "hex": lambda r, self: hex(int(self))[2:], # int to hex
  "sha256digest": lambda r, self: sha256(str(self).encode()).hexdigest(),
  "md5digest": lambda r, self: md5(str(self).encode()).hexdigest(),
  "round": lambda r, self: round(float(self)), # TODO: genericize to roundN
  "round2": lambda r, self: round(float(self), 2),
  "round4": lambda r, self: round(float(self), 4),
  "round6": lambda r, self: round(float(self), 6),
  "round8": lambda r, self: round(float(self), 8),
  "round10": lambda r, self: round(float(self), 10),
}

SERIES_TRANSFORMERS: dict[str, callable] = {
  "median": lambda r, series: np.median(series),
  "mean": lambda r, series: np.mean(series),
  "std": lambda r, series: np.std(series),
  "var": lambda r, series: np.var(series),
  "min": lambda r, series: np.min(series),
  "max": lambda r, series: np.max(series),
  "sum": lambda r, series: np.sum(series), # single cumulative sum
  "cumsum": lambda r, series: np.cumsum(series), # array of cumulative sums
  "prod": lambda r, series: np.prod(series)
}

# TODO: optimize
def apply_transformer(c: Collector, field: ResourceField, transformer: str) -> any:
  if not transformer:
    return field.value

  # Checks if single word and does not contain injected variables ('{' or '}')
  if bool(re.fullmatch(r"[^\s{}]+", transformer)):
    return BASE_TRANSFORMERS.get(transformer)(c, field.value)

  # If transformer contains a series op "::", replace with the result of the series transformer
  if "{" in transformer:
    # step 0: extract the series transformers that starts with '{' and ends with '}', and contain '::'
    search = re.search(r"\{(.+?)\}", transformer)
    for group in search.groups():
      if "::" in group:
        # step 1: identify the target series, word between '{' and '::'
        search = re.search(r"\{(.+?)::", transformer)
        target = search.group(1) if search else None
        # step 2: identify the transformer, word between '::' and '('
        search = re.search(r"::(.+?)\(", transformer)
        fn = search.group(1) if search else None
        # step 3: identify the lookback, word between '(' and ')'
        search = re.search(r"\((.+?)\)", transformer)
        lookback = search.group(1) if search else None
        # step 4: check integrity of the transformer
        if not target or not fn or not lookback:
          raise ValueError(f"Invalid transformer: {transformer}")
        # step 5: translate the lookback timeframe into a timedelta
        from_date = interval_to_delta(lookback, backwards=True)
        # step 6: make sure that target is either self (field) or other resource field
        if target == "self":
          target_field = field
        else:
          # filter the resource field that match the target
          target_field = next((f for f in c.fields if f.name == target), None)
          if not target_field:
            raise ValueError(f"Invalid transformer target: {target}")
        # step 7: extract the series from the target field
        series = load(c, from_date, interval=c.interval, field=target_field)
        # step 8: apply the series transformer
        res = SERIES_TRANSFORMERS.get(fn)(c, series)
        # step 9: inject the result in the transformer for recursive evaluation
        transformer = transformer.replace(group, res)
  # eval the transformer after injecting all computed values
  return safe_eval(transformer.format(self=field.value, **c.data_by_field)) # TODO: precompile

def transform(c: Collector, f: ResourceField) -> any:
  for t in f.transformers or []:
    f.value = apply_transformer(c, f, t)
  c.data_by_field[f.name] = f.value
  return f.value

def transform_all(c: Collector) -> int:
  tp = state.get_thread_pool()
  count = 0
  for field in c.fields:
    try:
      tp.submit(transform, c, field).result(timeout=2)
      count += 1
    except (FutureTimeoutError, Exception):
      log_error(f"{c.name}.{field.name} transformer timeout, check {c.collector_type} output and transformer chain")

  if state.args.verbose:
    log_debug(f"Transformed {c.name} -> {c.data_by_field}")
  return count
