
from argparse import _ArgumentGroup, ArgumentParser
from typing import Any, Literal
from os import environ as env
from dotenv import find_dotenv, load_dotenv

from src.utils import is_bool, prettify


class ArgParser(ArgumentParser):

  info: dict[str, tuple] = {}
  origin: dict[str, Literal['cli','env','default']]
  parsed: any = None

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.info = {}  # Internal map for storing argument info
    self.origin = {}
    self.parsed: any = None

  def add_argument(self, *args, **kwargs):
    if kwargs.get("group"):
      group = kwargs.pop("group")
      action = group.add_argument(*args, **kwargs)
    else:
      action = super().add_argument(*args, **kwargs)
    if not action.type:
      action.type = bool if is_bool(action.const or action.default) else (type(action.default) or str)
    self.info[action.dest] = (action.type, action.default) # arg type and default value
    return action

  def get_info(self, arg_name):
    return self.info.get(arg_name)

  def parse_args(self, *args, **kwargs) -> any:
    self.parsed = super().parse_args(*args, **kwargs)
    return self.parsed

  def argument_tuple_to_kwargs(self, arg_tuple: tuple) -> dict:
    names_tuple, arg_type, default, action, help_str = arg_tuple
    args = names_tuple
    kwargs = {
      "default": default,
      "help": help_str
    }
    if arg_type and not action:
      kwargs["type"] = arg_type
    if action:
      kwargs["action"] = action
    return args, kwargs

  def add_arguments(self, arguments: list[tuple], group: _ArgumentGroup=None) -> None:
    for arg_tuple in arguments:
      args, kwargs = self.argument_tuple_to_kwargs(arg_tuple)
      if group:
        kwargs["group"] = group
      self.add_argument(*args, **kwargs)

  def add_group(self, name: str, arguments: list[tuple]) -> None:
    group = self.add_argument_group(name)
    self.add_arguments(group=group, arguments=arguments)

  def add_groups(self, groups: dict[str, list[tuple]]) -> None:
    for group_name, arguments in groups.items():
      self.add_group(group_name, arguments)

  def load_env(self, path: str=None) -> any:
    if not self.parsed:
      self.parse_args()
    load_dotenv(find_dotenv(path or self.parsed.env))
    for k, v in vars(self.parsed).items():
      arg_type, arg_default = self.get_info(k)
      is_default = arg_default == v
      env_val = env.get(k.upper())
      if env_val and is_default:
        selected = arg_type(env_val) if arg_type != bool else env_val.lower() == "true"
        self.origin[k] = "env"
      else:
        selected = v
        env[k.upper()] = str(v) # inject into env for naive access
        self.origin[k] = "default" if is_default else "cli"
      setattr(self.parsed, k, selected)
    return self.parsed

  def pretty(self):
    rows = [
      [arg, getattr(self.parsed, arg), self.origin.get(arg, "unknown")]
      for arg in vars(self.parsed)
    ]
    return prettify(data=rows, headers=["Name", "Value", "Source"])
