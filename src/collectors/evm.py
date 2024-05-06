from dotenv import load_dotenv
import hashlib
import os

from web3 import Web3
from multicall import Call, Multicall

from ..model import CollectorConfig, ResourceField, TsdbAdapter

# Load relevant environment variables
load_dotenv('.env.test')

def result(value):
    return value

def evm_call_collect(ctx, db: TsdbAdapter, config: CollectorConfig) -> bool:
    # Prepare multi calls
    call_list = {}

    for _, evm_call in enumerate(config.data):
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
        query_hash = hashlib.sha1(f"{chain_id}:{target_addr}:{evm_call.selector}:{evm_call.parameters}".encode()).hexdigest()

        # Add call to multicall
        fn_call = Call(target=target_addr, function=[evm_call.selector, *evm_call.parameters], returns=[(f'{query_hash}', result)])
        call_list[chain_id].calls.append(fn_call)

    # Execute multicalls
    for chain_id in call_list.keys():
        output = call_list[chain_id]()
        print(f"[CHAIN {chain_id}: => {output}")

    return True