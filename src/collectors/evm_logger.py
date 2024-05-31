from asyncio import Task
from web3 import Web3

from src.model import Collector, ResourceField
from src.utils import log_debug, log_error, log_info, log_warn, split_chain_addr
from src.actions.store import store, transform_and_store
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state

def parse_event_signature(signature: str) -> tuple[str, list[str], list[bool]]:
  event_name, params = signature.split('(')
  param_list = params.rstrip(')').split(',')
  param_types = [param.split(' ')[-1] for param in param_list]
  indexed = ['indexed' in param for param in param_list]
  return event_name.strip(), param_types, indexed

def decode_log_data(client: Web3, log: dict, topics_first: list[str], indexed: list[bool]) -> tuple:
  topics = log['topics'][1:]
  data = bytes()
  for t in topics:
    data += t
  data += log['data']
  decoded = client.codec.decode(types=topics_first, data=data)
  return tuple(reorder_decoded_params(decoded, indexed))

def reorder_decoded_params(decoded: list, indexed: list[bool]) -> list:
  reordered = []
  index_count, non_index_count = 0, 0

  for is_indexed in indexed:
    if is_indexed:
      reordered.append(decoded[index_count])
      index_count += 1
    else:
      reordered.append(decoded[index_count + non_index_count])
      non_index_count += 1
  return reordered

async def schedule(c: Collector) -> list[Task]:

  contracts: set[str] = set()
  data_by_event: dict[str, dict] = {}
  index_first_types_by_event: dict[str, list[str]] = {}
  event_hashes: dict[str, str] = {}
  events_by_contract: dict[str, set[str]] = {}
  filter_by_contract: dict[str, dict] = {}
  filter_index_by_event: dict[str, int] = {}
  last_block_by_contract: dict[str, int] = {}

  for field in c.fields:
    contracts.add(field.target)
    events_by_contract.setdefault(field.target, set()).add(field.selector)

  for contract in contracts:

    chain_id, addr = split_chain_addr(contract)
    addr = Web3.to_checksum_address(addr) # enforce checksum

    for event in events_by_contract[contract].copy(): # copy to avoid modifying while iterating
      event_name, param_types, indexed = parse_event_signature(field.selector)
      index_types, non_index_types = [], []
      event_id = f"{field.target}:{field.selector}"

      event_hash = Web3.keccak(text=field.selector.replace('indexed ', '')).hex()
      event_hashes[event_id] = event_hash; event_hashes[event_hash] = event_id

      for i, is_indexed in enumerate(indexed):
        index_types.append(param_types[i]) if is_indexed else non_index_types.append(param_types[i])
      index_first_types_by_event[event_id] = list(index_types) + non_index_types

      events_by_contract.setdefault(field.target, set()).add(event_id)
      data_by_event.setdefault(event_id, {})

      filter_by_contract.setdefault(field.target, {
        "fromBlock": "latest",
        "toBlock": "latest",
        "address": addr,
        "topics": []
      })["topics"].append(event_hashes[event_id])

      filter_index_by_event[event_id] = len(filter_by_contract[field.target]["topics"]) - 1

    filter_by_contract[contract]["topics"] = list(set(filter_by_contract[contract]["topics"])) # remove duplicates

  def poll_events(contract: str):
    chain_id, addr = split_chain_addr(contract)
    client = state.get_web3_client(chain_id, rolling=True)
    f = filter_by_contract[contract]
    retry_count = 0
    output = None
    current_block = client.eth.block_number
    last_block_by_contract.setdefault(contract, current_block)
    prev_block = last_block_by_contract[contract]
    while not output and retry_count < state.args.max_retries:
      if state.args.verbose:
        log_debug(f"Polling for {contract} events...")
      start_block = prev_block
      end_block = current_block
      f.update({"fromBlock": hex(start_block), "toBlock": hex(end_block)})
      if start_block >= end_block:
        log_info(f"No new blocks for {contract}, skipping event polling for {c.interval}")
        break
      try:
        logs = client.eth.get_logs(f)
        for l in logs:
          event_id = l["topics"][0]
          decoded_event = decode_log_data(client, l, index_first_types_by_event[event_id], indexed)
          if state.args.verbose:
            log_debug(f"Block: {l['blockNumber']} | Event: {decoded_event}")
        start_block = end_block + 1
      except Exception as l:
        log_error(f"Failed to poll event logs for contract {c}: {l}")
        client = state.get_web3_client(chain_id, rolling=True)
        retry_count += 1

  async def collect(c: Collector):
    await ensure_claim_task(c)

    future_by_contract = {}
    tp = state.get_thread_pool()
    for contract in contracts:
      future_by_contract[contract] = tp.submit(poll_events, contract)

    for field in c.fields:
      try:
        field.value = future_by_contract[field.target].result(timeout=3)
        c.data_by_field[field.name] = field.value
      except Exception as e:
        log_error(f"Failed to poll events for {field.target}: {e}")

    if state.args.verbose:
      log_debug(f"Collected {c.name} -> {c.data_by_field}")

    await transform_and_store(c)

  # globally register/schedule the collector
  return [await state.scheduler.add_collector(c, fn=collect, start=False)]
