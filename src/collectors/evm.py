from asyncio import Task
from multicall import Call, Multicall
from src.model import Collector, ResourceField
from src.utils import floor_utc, interval_to_seconds, log_debug, log_error, log_warn
from src.actions.store import store
from src.actions.transform import transform
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state

def parse_generic(data: any) -> any:
  return data

async def schedule(c: Collector) -> list[Task]:

  async def collect(c: Collector):
    ensure_claim_task(c)
    unique_calls = set()
    calls_by_chain = {}
    field_by_name = {f.name: f for f in c.data}

    for field in c.data:
      if not field.target:
        log_error(f"Missing target smart contract view for field {c.name}.{field.name}, skipping...")
        continue
      if field.id in unique_calls:
        log_warn(f"Duplicate target smart contract view {field.target} in {c.name}.{field.name}, skipping...")
        continue
      unique_calls.add(field.id)

      chain_id, addr = field.chain_addr()
      if chain_id not in calls_by_chain:
        client = state.get_web3_client(chain_id)
        calls_by_chain[chain_id] = Multicall(calls=[], _w3=client)

      call = Call(target=addr, function=[field.selector, *field.params], returns=[[field.name, parse_generic]])
      calls_by_chain[chain_id].calls.append(call)

    for chain_id, multicall in calls_by_chain.items():
      try:
        ft = state.get_thread_pool().submit(multicall)
        output = ft.result()
        for name, value in output.items():
          field = field_by_name.get(name)
          field.value = value # can be a tuple
          c.data_by_field[field.name] = field.value
          if state.verbose:
            log_debug(f"Collected {c.name}.{field.name} -> {field.value}")
      except Exception as e:
        log_error(f"Failed to execute multicall for chain {chain_id}: {e}")

    # run transformers field by field sequentially as described after chain by chain multicall execution
    for field in c.data:
      if field.value and field.transformers:
        field.value = transform(c, field)

    c.collection_time = floor_utc(c.interval)
    await store(c)

  # globally register/schedule the collector
  return [state.add_cron(c.id, fn=collect, args=(c,), interval=c.interval)]

