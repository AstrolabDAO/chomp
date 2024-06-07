def is_bool(value: any) -> bool:
  return str(value).lower() in ["true", "false", "yes", "no", "1", "0"]

def to_bool(value: str) -> bool:
  return value.lower() in ["true", "yes", "1"]

def is_float(s: str) -> bool:
  try:
    float(s)
    return True
  except ValueError:
    return False
