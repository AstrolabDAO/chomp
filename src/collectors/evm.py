from asyncio import Task
from multicall import Call, Multicall
from concurrent.futures import TimeoutError as FutureTimeoutError

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
    await ensure_claim_task(c)
    unique_calls, calls_by_chain = set(), {}
    field_by_name = {f.name: f for f in c.data}

    for field in c.data:
      if not field.target or field.id in unique_calls:
        if field.id in unique_calls:
          log_warn(f"Duplicate target smart contract view {field.target} in {c.name}.{field.name}, skipping...")
        continue
      unique_calls.add(field.id)

      chain_id, addr = field.chain_addr()
      if chain_id not in calls_by_chain:
        client = await state.get_web3_client(chain_id, rolling=True)
        calls_by_chain[chain_id] = Multicall(calls=[], _w3=client, require_success=True) # gas_limit=50_000_000
      calls_by_chain[chain_id].calls.append(Call(target=addr, function=[field.selector, *field.params], returns=[[field.name, parse_generic]]))

    tp = await state.get_thread_pool()
    for chain_id, multicall in calls_by_chain.items():
      try:
        output = None
        retry_count = 0
        while not output and retry_count < state.max_retries:
          try:
            output = tp.submit(multicall).result(timeout=3.5)
          except Exception as e: # (TimeoutError, FutureTimeoutError):
            log_error(f"Multicall for chain {chain_id} failed: {e}, switching RPC...")
            multicall.w3 = await state.get_web3_client(chain_id, rolling=True)
            retry_count += 1

        if not output:
            log_error(f"Failed to execute multicall for chain {chain_id} after {state.max_retries} retries.")
            continue

        for name, value in output.items():
          field = field_by_name.get(name)
          field.value = value
          c.data_by_field[field.name] = field.value

      except Exception as e:
        log_error(f"Failed to execute multicall for chain {chain_id}: {e}")

    if state.verbose:
      log_debug(f"Collected {c.name} -> {c.data_by_field}")

    for field in c.data:
      if field.transformers:
        try:
          tp.submit(transform, c, field).result(timeout=2)
        except (FutureTimeoutError, Exception):
          log_error(f"{c.name}.{field.name} transformer timeout, check multicall output and transformer chain")

    if state.verbose:
      log_debug(f"Transformed {c.name} -> {c.data_by_field}")

    c.collection_time = floor_utc(c.interval)
    await store(c)

  # globally register/schedule the collector
  return [state.add_cron(c.id, fn=collect, args=(c,), interval=c.interval)]
