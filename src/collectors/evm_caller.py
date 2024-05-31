from asyncio import Task
from multicall import Call, Multicall, constants as mc_const

from src.model import Collector, ResourceField
from src.utils import log_debug, log_error, log_warn
from src.actions.store import store, transform_and_store
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state

def parse_generic(data: any) -> any:
  return data

async def schedule(c: Collector) -> list[Task]:

  async def collect(c: Collector):
    await ensure_claim_task(c)
    unique_calls, calls_by_chain = set(), {}
    field_by_name = {f.name: f for f in c.fields}

    for field in c.fields:
      if not field.target or field.id in unique_calls:
        if field.id in unique_calls:
          log_warn(f"Duplicate target smart contract view {field.target} in {c.name}.{field.name}, skipping...")
        continue
      unique_calls.add(field.id)

      chain_id, addr = field.chain_addr()
      if chain_id not in calls_by_chain:
        client = state.get_web3_client(chain_id, rolling=True)
        calls_by_chain[chain_id] = Multicall(calls=[], _w3=client, require_success=True, gas_limit=mc_const.GAS_LIMIT)
      calls_by_chain[chain_id].calls.append(Call(target=addr, function=[field.selector, *field.params], returns=[[field.name, parse_generic]]))

    tp = state.get_thread_pool()

    def call_multi(m: Multicall):
      try:
        output = None
        retry_count = 0
        while not output and retry_count < state.args.max_retries:
          try:
            output = tp.submit(m).result(timeout=3)
            return output
          except Exception as e: # (TimeoutError, FutureTimeoutError):
            log_error(f"Multicall for chain {chain_id} failed: {e}, switching RPC...")
            m.w3 = state.get_web3_client(chain_id, rolling=True)
            retry_count += 1

        if not output:
            log_error(f"Failed to execute multicall for chain {chain_id} after {state.args.max_retries} retries.")
            return {}

      except Exception as e:
        log_error(f"Failed to execute multicall for chain {chain_id}: {e}")

    futures = []
    for chain_id, m in calls_by_chain.items():
      futures.append(tp.submit(call_multi, m))

    for future in futures:
      output = future.result() # max 3s timeout
      for name, value in output.items():
        field = field_by_name.get(name)
        field.value = value
        c.data_by_field[field.name] = field.value

    if state.args.verbose:
      log_debug(f"Collected {c.name} -> {c.data_by_field}")

    await transform_and_store(c)

  # globally register/schedule the collector
  return [await state.scheduler.add_collector(c, fn=collect, start=False)]
