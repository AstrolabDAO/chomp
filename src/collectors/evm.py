from hashlib import md5
import os
from utils import log_debug
from web3 import Web3
from multicall import Call, Multicall

from src.model import Collector, ResourceField, Tsdb

def result(value):
  return value

def collect(c: Collector) -> bool:
  # Prepare multi calls
  call_list = {}

  for _, evm_call in enumerate(c.data):
    chain_id, target_addr = evm_call.target.split(":")

    # Checksum to be sure
    target_addr = Web3.to_checksum_address(target_addr)

    # Check if chain_id already exists in call_list
    if chain_id not in call_list.keys():
      # Initialize a new client for chain_id & setup multicall object
      rpc = os.getenv(f"{chain_id}_RPC")
      w3_client = Web3(Web3.HTTPProvider(rpc))

      assert w3_client.is_connected(), f"Could not connect to RPC at {rpc}"

      call_list[chain_id] = Multicall(calls=[], _w3=w3_client)

    # Generate query hash - nice to have
    query_hash = md5(f"{chain_id}:{target_addr}:{evm_call.selector}:{evm_call.parameters}".encode()).hexdigest()

    # Add call to multicall
    fn_call = Call(target=target_addr, function=[evm_call.selector, *evm_call.parameters], returns=[(f'{query_hash}', result)])
    call_list[chain_id].calls.append(fn_call)

  # Execute multicalls
  for chain_id in call_list.keys():
    output = call_list[chain_id]()
    log_debug(f"[CHAIN {chain_id}: => {output}")

  return True
