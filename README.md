<div align="center">
  <img style="border-radius=25px; max-height=250px;" height="400" src="./banner.png" />
  <!-- <h1>Chomp</h1> -->
  <p>
    <a href="https://t.me/chomp_ingester"><img alt="Chomp" src="https://img.shields.io/badge/Telegram-chomp_ingester-blue?logo=telegram">
    <a href="https://discord.gg/xEEHAY2v5t"><img alt="Discord Chat" src="https://img.shields.io/discord/984518964371673140?label=Astrolab%20DAO&logo=discord"/></a>
    <a href="https://opensource.org/licenses/MIT"><img alt="License" src="https://img.shields.io/github/license/AstrolabDAO/chomp?color=3AB2FF" /></a>
    <!-- <a href="https://docs.astrolab.fi"><img alt="Astrolab Docs" src="https://img.shields.io/badge/astrolab_docs-F9C3B3" /></a> -->
  </p>
  <!-- <p>
    <strong>by <a href="https://astrolab.fi">Astrolab DAO</a> & friends</strong>
  </p> -->
</div>

## Overview

Chomp is a small creature with unquenchable craving for data.

#### Chomp is:
- A highly modular data collector designed to retrieve, transform and archive data from Web2 and Web3 sources.
It allows anyone to set up a data ingestion back-end and pipelines in minutes, all from a simple YAML configuration file.
- Lightweight, you can self-host Chomp ingesters on a Raspberry Pi 4, its built-in sync makes it cluster-native.
- A faster alternative to [Ponder](https://ponder.sh/) and [TheGraph](https://thegraph.com/) if you need to start tracking your protocol's activity in real time without GraphQL.
- Plug and play, test it now! `cd ./setup && ./test-setup.bash`

#### Chomp is not:
- A graph indexer, it specializes in 1-dimensional timeseries data ingestion.
Graph indexing and API generation can however easily be implemented on top of it. (cf. [Contributing](#contributing))
- An all purpose task scheduler, it is a generic data back-end that deploys light ETL pipelines.
If you need domain specific task automation and consider using Chomp as all-purpose scheduler, [let's discuss it!](https://t.me/chomp_ingester)

## Features

- **Multimodal Collection:** Simultaneously ingest data from web APIs, webpages, blockchains, and more.
- **Low Code, Config Based:** Start collecting data by editing a [single YAML file](#general-structure), no more code or config heavy manoeuvres.
- **Light and Self-Hostable:** Can be deployed on devices with minimal resources, such as a Raspberry Pi.
- **Native Clustering:** [Just spawn multiple instances](#clustering) using the same Redis and watch them sync, no extra config required.

## Use Cases

- **Web3 DApp Backends:** Set up a data backend for your decentralized app in minutes, [EVM or not](#web3-caller-and-logger-specific-evm-solana-sui-aptos-ton).
- **Data Aggregators:** Collect and consolidate data from various [on-chain](#web3-_caller-and-_logger-specific-evm-solana-sui-aptos-ton) and [off-chain](#http_api-and-ws_api-specific) sources for analysis.
- **Homelab Stuff:** Integrate and manage data collection for home servers and IoT devices.
- **Mass Metrics Ingestion:** Gather and process metrics from diverse systems and platforms, all at once.

## Disclaimers

- **Work in Progress:** As per the [licence](./LICENCE), the project is under development. All of its aspects may change and improve over time.
- **Rate Limits:** Mind RPC and HTTP endpoints rate limits. This usually does not apply to WebSocket and FIX connections.
- **Data Storage Size:** Ingestion table sizes can grow rapidly in expansive setups (many collectors/fields and short intervals). Ensure adequate storage planning, db schemas and compression settings (or just use the [default TDengine+Redis setup](./setup/Dockerfile.db)).

## Installation Guide

Chomp needs a minimal back-end to work:
- a Redis to synchronize jobs across instances and cache data
- a database, preferably columnar and timeseries oriented

If you already have a compatible database and/or Redis running, you can configure it in the used `.env`.
For more information, see [CLI arguments and .env](#cli-arguments-and-env)

The [default database adapter is TDengine's](./setup/Dockerfile.db), but any can be implemented in [./src/adapters](./src/adapters).
The following have been drafted, but remain untested
- [Timescale](./src/adapters/timescale.py)
- [OpenTSDB](./src/adapters/opentsdb.py)
- [MongoDB](./src/adapters/mongodb.py) (using timeseries collections)
- [KDB (KX)](./src/adapters/kx.py)

### Default Backend Docker Setup

1. **Prepare the Environment:**
   Ensure Docker is installed on your system.

2. **Build the Backend Image, Start it, Test it:**
   ```sh
   cd ./setup
   bash ./db-setup.bash
   ```

### Local Installation with PDM

0. **Clone the Repository:**
   ```sh
   git clone https://github.com/yourusername/chomp.git
   cd chomp
   ```

### Manual installation

Make sure Python3 and pip are installed and aliased to `python` and `pip`.
[Learn how to install Python here.](./https://www.python.org/)

1. **Install and pdm if missing:**

  ```sh
  pip install pdm
  ```

2. **Install Dependencies:**

  ```sh
  pdm install
  ```

3. **Run as ingestion node:**

  ```sh
  pdm python main.py -e .env.example
  ```

#### Clustering

Just spawn more instances, they'll automatically sync and pick up leftover jobs

```bash
for i in {1..5}; do pdm python main.py -e .env.example -j 5 & sleep 5; done
```

#### Quick Setup

1. **All of the above at once:**

  ```bash
  cd ./setup && ./test-setup.bash
  ```

### Docker Image Setup (Docker/Kubernetes)

🚧: A Chomp docker image will be available at [./setup/Dockerfile.worker](./setup/Dockerfile.worker)

## Configuration

### CLI arguments and .env

All of the runtime config parameters are accepted both as .env variables (upper snake case) and CLI arguments (snake case).

#### Environment Variables

```bash
# runtime config
MAX_JOBS=10                 # max collection jobs by instance
MAX_RETRIES=6               # max retries on ingestion failure
RETRY_COOLDOWN=5            # min cooldown between ingestion retries
LOGFILE=out.log
CONFIG_PATH=./resources.yml # schemas (resources) definition file
TSDB_ADAPTER=tdengine       # instance db back-end
THREADED=true               # multithread ingestion/transformation jobs
PERPETUAL_INDEXING=false    # never stop polling for web3 events (`*_logger` collectors), paid-for RPCs only

# db settings
DB_RW_USER=rw               # back-end db+redis service account
DB_RW_PASS=pass             # back-end db+redis service pass

REDIS_HOST=localhost        # back-end redis host
REDIS_PORT=40001            # back-end redis port
REDIS_DB=0                  # back-end redis db id

TAOS_HOST=localhost         # back-end db host
TAOS_PORT=40002             # back-end db port
TAOS_HTTP_PORT=40003        # tdengine specific http autogen api port
TAOS_DB=chomp               # back-end db name

# chains rpcs formatted as `{id}_{protocol}_RPCS`, comma separated
1_HTTP_RPCS=rpc.ankr.com/eth,eth.llamarpc.com,1rpc.io/eth
10_HTTP_RPCS=rpc.ankr.com/optimism,optimism.llamarpc.com,1rpc.io/op
56_HTTP_RPCS=rpc.ankr.com/bsc,binance.llamarpc.com,bsc-mainnet.public.blastapi.io
100_HTTP_RPCS=rpc.ankr.com/gnosis,gnosis-mainnet.public.blastapi.io,1rpc.io/gnosis
137_HTTP_RPCS=rpc.ankr.com/polygon,polygon-mainnet.public.blastapi.io,1rpc.io/matic
238_HTTP_RPCS=rpc.ankr.com/blast,rpc.blastblockchain.com,blast.drpc.org
```

#### CLI Arguments

For usage and more CLI info, information, run `python main.py -h`.

- `-e`, `--env`: Environment file if any (default: ".env")
- `-c`, `--config_path`: Collectors YAML configuration file (default: "./examples/dex-vs-cex.yml")
- `-v`, `--verbose`: Verbose output (loglevel debug)
- `-i`, `--proc_id`: Unique instance identifier (default: "chomp-{generated_hash}")
- `-a`, `--tsdb_adapter`: Timeseries database adapter (default: "tdengine")
- `-j`, `--max_jobs`: Max collector jobs to run concurrently (default: 16)
- `-r`, `--max_retries`: Max collector retries per event, applies to fetching/querying (default: 5)
- `-co`, `--retry_cooldown`: Min sleep time between retries, in seconds (default: 2)
- `-t`, `--threaded`: Run jobs in separate threads (default: true)
- `-p`, `--perpetual_indexing`: Perpetually listen for new blocks to index, requires capable RPCs (default: false)

### Collectors Configuration

Chomp is config-based, and as such, its only limit is your capability to configure it.
The collectors config file is passed by path with the `-c` or `--config_path` flag, or `CONFIG_PATH` env variable.
Rest assured, if your file is not well formatted, explicit validation errors will let you know as you run Chomp.

For an in-depth understanding of Chomp's configuration, please refer to its [yamale](https://github.com/23andMe/Yamale) validation schema: [./src/config-schema.yml](./src/config-schema.yml) and [./src/model.py](./src/model.py).

#### General Structure

  ```yaml
  scrapper: []
  http_api:
    - name: ExampleCollector          # unique resource name (mapped to db table)
      resource_type: timeseries       # defaults to time indexing
      target: http://example.com/api  # collector target
      interval: m1                    # collection interval
      fields:                         # data fields (mapped to db columns)
        - name: text1                     # unique resource attribute
          type: string                    # db storage type/format
          selector: .data.text1           # target data selector
          transformers: ["strip"]         # self-referenciable transformer chain
        - name: number1
          type: float64
          transformers: ["{self}", "round6"]
        - name: squaredNumber1
          type: float64
          transformers: ["{number1} ** 2", "round6"]
  ws_api: []
  evm_caller: []
  evm_logger: []
  ...
  ```

#### Generic Collector Attributes

- **resource_type:** Collection/indexing type, any of `timeseries` or `value` (inplace values, FIFO)
- **target:** Resource target - eg. URL, contract address.
- **selector:** Field query/selector.
- **fields:** Defines the data fields to collect.
- **type:** Resource or field storage type, any of `int8` `uint8` `int16` `uint16` `int32` `uint32` `int64` `uint64` `float32` `ufloat32` `float64` `ufloat64` `bool` `timestamp` `string` `binary` `varbinary`

#### `scrapper` specific

- **target:** The web page URL (e.g., `http://example.com/page1`).
- **selector:** XPath or CSS selector.

#### `http_api` and `ws_api` specific

- **target:** The API URL (e.g., `http://example.com/api`).
- **selector:** Nested attribute selector.

#### web3 `*_caller` and `*_logger` specific (evm, solana, sui, aptos, ton)

- **target:** The chain ID and contract address, colon delimited (e.g., `1:0x1234...`).
- **selector:** Contract method for `evm_caller`, event signature for `evm_logger`.
- **fields:** Specifies the fields to extract from contract calls or events, with types and transformers.

## Comparison with Similar Tools

| Feature | Chomp | Ponder.sh | The Graph |
|---------|-------|-----------|-----------|
| **___ APIs ___** |
| REST API | partia | ❌ | ❌ |
| SQL API | ✔️ | ✔️ | ❌ |
| GraphQL API | ❌ | ✔️ | ✔️ |
| **___ Collectors ___** |
| HTTP API | ✔️ | ❌ | ❌ |
| WS API | ✔️ | ❌ | ❌ |
| FIX API | 🚧 | ❌ | ❌ |
| EVM Logs | ✔️ | ✔️ | ✔️ |
| EVM Reads | ✔️ | ✔️ | ❌ |
| EVM Call Traces | 🚧 | ✔️ | ✔️ |
| EVM Blocks | ❌ | ❌ | ✔️ |
| Solana Reads | 🚧 | ❌ | ✔️ |
| Solana Logs | 🚧 | ❌ | ✔️ |
| Sui Reads | 🚧 | ❌ | ❌ |
| Sui Logs | 🚧 | ❌ | ❌ |
| Aptos Reads | 🚧 | ❌ | ❌ |
| Aptos Logs | 🚧 | ❌ | ❌ |
| Ton Reads | 🚧 | ❌ | ❌ |
| Ton Logs | 🚧 | ❌ | ❌ |
| **__ Features __** |
| Dashboard | 🚧 | ❌ | ✔️ |
| No-code Schema Declaration | ✔️ | ❌ | ✔️ |
| Auto-Scaling | ✔️ | ❌ | ✔️ |

### Differences with Ponder.sh and The Graph

#### Ponder.sh

- **++Lightweight:** Ponder's codebase is easy to comprehend, extend, and not very resource intensive for small schemas despite lack of IO optimization.
- **+-APIs:** Offers GraphQL and SQL APIs, but lacks REST support and dashboard/schema explorer.
- **--Row Oriented:** Uses SQLite or Postgres as back-ends, which are not ideal for big time-series storage.
- **--Web3 Only:** Limited Web3 data collection.

#### The Graph

- **++Managed:** Hosted, decentralized and managed, therefore low-maintenance.
- **++Documented:** The maturity is felt in TheGraph's documentation and community.
- **+-APIs:** Provides a user friendly dashboard, as well as a proven GraphQL API, but no REST or SQL support.
- **+-Web3 Only:** Limited to Web3 (EVMs and Solana) data collection, but fine grained: function calls/events/block mint hooks are all available.
- **--Complex Deployment:** The Graph is more resource-intensive and not that straightforward to self-host.

## Contributing

Contributions are much welcome!

Chomp is currently mostly an multimodal ingester, and lacks:
- Websocket API forwarder - directly forward data from [Redis](https://redis.io/) PubSub to consumers using [FastAPI](https://fastapi.tiangolo.com/) 🚧 [./src/server/forwarder.py](./src/server/forwarder.py)
- Rest API generation - dynamically generate and expose stored data on endpoints using [FastAPI](https://fastapi.tiangolo.com/)
- [GraphQL](https://graphql.org/) adapter - expose stored data similarly to [TheGraph](https://thegraph.com/) using [Strawberry](https://strawberry.rocks/)
- UI to explore running nodes, configure collectors configurations, monitor data feeds
- Adapters - currently [TDengine](https://tdengine.com/) was our focus for performance and stability purposes, but new database adapters can very easily be added to [./src/adapters](./src/adapters), we are already looking at [Timescale](https://www.timescale.com/), [Influx](https://www.influxdata.com/), [kdb/kx](https://kx.com/) and others
- Performance profiling and optimization (better IO, threading, transformers, or even a [Rust](https://www.rust-lang.org/) port)

## License
This project is licensed under the MIT License, use at will. ❤️
