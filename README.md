<div align="center">
  <img style="border-radius=25px; max-height=250px;" height="400" src="./banner.png" />
  <!-- <h1>Chomp</h1> -->
  <p>
    <a href="https://t.me/chomp_ingester"><img alt="Chomp" src="https://img.shields.io/badge/Chomp--white?style=social&logo=telegram">
    <a href="https://discord.gg/xEEHAY2v5t"><img alt="Discord Chat" src="https://img.shields.io/badge/Astrolab%20DAO--white?logo=discord&style=social"/></a>
    <a href="https://twitter.com/AstrolabDAO"><img alt="Twitter Follow" src="https://img.shields.io/twitter/follow/AstrolabDAO?label=@AstrolabDAO&style=social"></a>
    <!-- <a href="https://docs.astrolab.fi"><img alt="Astrolab Docs" src="https://img.shields.io/badge/astrolab_docs-F9C3B3" /></a> -->
    <a href="https://opensource.org/licenses/MIT"><img alt="License" src="https://img.shields.io/github/license/AstrolabDAO/chomp?style=social" /></a>
  </p>
  <!-- <p>
    <strong>by <a href="https://astrolab.fi">Astrolab DAO</a> & friends</strong>
  </p> -->
</div>

## Overview

Chomp is a small creature with unquenchable craving for data.

#### Chomp is:
- A productivity-first, highly modular data ingester that retrieves, transforms and archives data from Web2 and Web3 sources.
It allows anyone to set up a data back-end and ETL pipelines in minutes, all from a simple YAML configuration file.
- Lightweight, you can self-host Chomp on a Raspberry Pi 4, its built-in sync makes it cluster-native.
- A faster alternative to [Ponder](https://ponder.sh/) and [TheGraph](https://thegraph.com/) if you need to track your protocol's activity in real time.
- Plug and play, test it now! `cd ./setup && ./test-setup.bash`
- An out-of-the-box http and websocket API to expose all your ingested data

#### Chomp is not:
- A graph indexer, it specializes in 1-dimensional timeseries data ingestion.
Graph indexing and API generation can however easily be implemented on top of it. (cf. [Contributing](#contributing))
- An all purpose task scheduler, it is a generic data back-end that deploys light ETL pipelines.
If you need domain specific task automation and consider using Chomp as all-purpose scheduler, [let's discuss it!](https://t.me/chomp_ingester)

## Features

- **Multimodal Ingestion:** Simultaneously consume data from web APIs, webpages, blockchains, and more.
- **Low Code, Config Based:** Start collecting data by editing a [single YAML file](#general-structure), no more code or config heavy manoeuvres.
- **Light and Self-Hostable:** Can be deployed on devices with minimal resources, such as a Raspberry Pi.
- **Native Clustering:** [Just spawn multiple instances](#clustering) using the same Redis and watch them sync, no extra config required.

## Use Cases

- **Web3 DApp Backends:** Set up a data backend for your decentralized app in minutes, [EVM or not](#web3-caller-and-logger-specific-evm-solana-sui-aptos-ton).
- **Data Aggregators:** Ingest and consolidate data from various [on-chain](#web3-_caller-and-_logger-specific-evm-solana-sui-aptos-ton) and [off-chain](#http_api-and-ws_api-specific) sources for analysis.
- **Homelab Stuff:** Integrate and manage data ingestion for home servers and IoT devices.
- **Mass Metrics Ingestion:** Gather and process metrics from diverse systems and platforms, all at once.

## Disclaimers

- **Work in Progress:** As per the [licence](./LICENCE), the project is under development. All of its aspects may change and improve over time.
- **Rate Limits:** Mind RPC and HTTP endpoints rate limits. This usually does not apply to WebSocket and FIX connections.
- **Data Storage Size:** Ingestion table sizes can grow rapidly in expansive setups (many resources/fields and short intervals). Ensure adequate storage planning, db schemas and compression settings (or just use the [default TDengine+Redis setup](./setup/Dockerfile.db)).

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


### Quick Setup

Test a full setup (clustered ingesters + server), all at once:

```bash
cd ./setup && sudo bash ./test-setup.bash
```

### Default Backend Docker Setup

1. **Prepare the Environment:**
   Ensure Docker is installed on your system.

2. **Build the Backend Image, Start it, Test it:**
   ```bash
   cd ./setup
   bash ./db-setup.bash
   ```

### Local Installation with PDM

0. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/chomp.git
   cd chomp
   ```

### Manual Installation

Make sure that the following dependencies are installed:
- Python (>= 3.10) and pip aliased to `python` and `pip` [cf. official docs](./https://www.python.org/)
- Docker (>= 21.0) and aliased to `docker` [cf. official docs](https://docs.docker.com/engine/install/ubuntu/)
- TDengine client for local installs with TDengine back-end default [cf. official docs](https://docs.tdengine.com/get-started/package/)
NB: the taos client should be compatible with [./setup/Dockerfile.db:{taos_image_version}](./setup/Dockerfile.db) eg. 3.2.3.0
- Redis client [cf. official docs](https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/install-redis-on-linux/) for local installs

1. **Install and pdm if missing:**

  ```bash
  pip install pdm
  ```

2. **Install Dependencies:**

  ```bash
  pdm install
  ```

### Runtime

#### Ingester Mode

##### Single Instance

```bash
pdm run python main.py -e .env.test -c ./examples/diverse.yml -j 16
```

##### Clustering

Just spawn more instances, they'll automatically sync and pick up leftover jobs

```bash
for i in {1..5}; do pdm run python main.py -e .env.test -j 5 & sleep 5; done
```

#### Server Mode

Just add the `-s`/`--server` flag to start a server instance.
It will expose all of `-c` ingested resources, both as http api and websocket api.

```bash
pdm run python main.py -e .env.test -c ./examples/diverse.yml --server
```

### Docker Image Setup (Docker/Kubernetes)

🚧: A Chomp docker image will be available at [./setup/Dockerfile.worker](./setup/Dockerfile.worker)

## Configuration

Chomp is config-based, and as such, its only limit is your capability to configure it.
Most of the runtime config parameters are accepted both as .env variables (upper snake case) and CLI arguments (snake case, overriding .env).

### Common Runtime

RPCs lists and database connection details are not expected on the cli (.env only)

#### CLI
- `-e, --env`: Environment file if any (default: `.env`) eg. `-e /path/to/.env`
- `-c, --config_path`: Ingesters YAML configuration file (default: `./examples/dex-vs-cex.yml`) eg. `-c /path/to/config.yml`
- `-v, --verbose`: Verbose output (loglevel debug) eg. `-v`
- `-i, --proc_id`: Unique instance identifier (default: `chomp-{random_hash}`) eg. `-i my_unique_id`
- `-r, --max_retries`: Max ingester retries per event, applies to fetching/querying (default: 5) eg. `-r 10`
- `-rc, --retry_cooldown`: Min sleep time between retries, in seconds (default: 2) eg. `-rc 5`
- `-t, --threaded`: Run jobs/routers in separate threads eg. `-t`
- `-a, --tsdb_adapter`: Timeseries database adapter (default: `tdengine`) eg. `-a influxdb`

#### .env
```env
LOGFILE=out.log           # Log file path
MAX_RETRIES=5             # Maximum retries for ingestion events
RETRY_COOLDOWN=5          # Minimum cooldown time between retries in seconds
THREADED=true             # Run jobs/routers in separate threads
TSDB_ADAPTER=tdengine     # Timeseries database adapter
CONFIG_PATH=./examples/diverse.yml  # Path to ingesters YAML configuration file

# cache/database settings
DB_RW_USER=rw             # Database read/write user
DB_RW_PASS=pass           # Database read/write password

REDIS_HOST=localhost      # Redis host
REDIS_PORT=40001          # Redis port
REDIS_DB=0                # Redis database number

TAOS_HOST=localhost       # TDengine host
TAOS_PORT=40002           # TDengine port
TAOS_HTTP_PORT=40003      # TDengine HTTP port
TAOS_DB="chomp"           # TDengine database name

# evm/non-evm rpc endpoints by id
1_HTTP_RPCS=rpc.ankr.com/eth,eth.llamarpc.com,endpoints.omniatech.io/v1/eth/mainnet/public
10_HTTP_RPCS=mainnet.optimism.io,rpc.ankr.com/optimism,optimism.llamarpc.com
56_HTTP_RPCS=bsc-dataseed.bnbchain.org,rpc.ankr.com/bsc,binance.llamarpc.com
100_HTTP_RPCS=rpc.gnosischain.com,rpc.ankr.com/gnosis,endpoints.omniatech.io/v1/gnosis/mainnet/public
137_HTTP_RPCS=rpc-mainnet.matic.network,rpc.ankr.com/polygon,endpoints.omniatech.io/v1/matic/mainnet/public
238_HTTP_RPCS=rpc.ankr.com/blast,rpc.blastblockchain.com,blast.drpc.org
250_HTTP_RPCS=rpc.fantom.network,rpc.ankr.com/fantom,1rpc.io/ftm
1284_HTTP_RPCS=rpc.api.moonbeam.network,rpc.ankr.com/moonbeam,endpoints.omniatech.io/v1/moonbeam/mainnet/public
5000_HTTP_RPCS=rpc.mantle.xyz,rpc.ankr.com/mantle,1rpc.io/mantle
8453_HTTP_RPCS=mainnet.base.org,base.llamarpc.com,1rpc.io/base
42161_HTTP_RPCS=arb1.arbitrum.io/rpc,arbitrum.llamarpc.com,rpc.ankr.com/arbitrum
43114_HTTP_RPCS=api.avax.network/ext/bc/C/rpc,rpc.ankr.com/avalanche,1rpc.io/avax/c
59144_HTTP_RPCS=rpc.linea.build,1rpc.io/linea,linea.drpc.org
534352_HTTP_RPCS=rpc.scroll.io,rpc.ankr.com/scroll,1rpc.io/scroll
```

### Ingester Runtime

When ran without `-s`/`--server` flag, every Chomp instance is by default an ingester.
It will pick-up ingestion jobs sequentially as defined by `-c` ingester's config file, in the limit of `-j` defined max jobs.

#### cli
- `-j, --max_jobs`: Max ingester jobs to run concurrently (default: 16) eg. `-j 20`
- `-p, --perpetual_indexing`: Perpetually listen for new blocks to index, requires capable RPCs eg. `-p`

#### .env
```env
MAX_JOBS=15               # Maximum ingester jobs to run concurrently
PERPETUAL_INDEXING=false  # Perpetually listen for new blocks to index
```

### Server Runtime

When ran with `-s`/`--server` flag, the Chomp instance starts in server mode.
To learn more, check the [server mode section](#server-mode).

#### cli
- `-s, --server`: Run as server (ingester by default) eg. `-s`
- `-sh, --host`: FastAPI server host (default: `127.0.0.1`) eg. `-sh 0.0.0.0`
- `-sp, --port`: FastAPI server port (default: 8000) eg. `-sp 8080`
- `-wpi, --ws_ping_interval`: WebSocket server ping interval (default: 30) eg. `-wpi 60`
- `-wpt, --ws_ping_timeout`: WebSocket server ping timeout (default: 20) eg. `-wpt 40`

#### .env
```env
SERVER_PORT=40004         # Server port
SERVER_HOST=localhost     # Server host
WS_PING_INTERVAL=30       # WebSocket server ping interval
WS_PING_TIMEOUT=20        # WebSocket server ping timeout
```

### Ingesters Config File (.yml)

The resources config file is passed by path with the `-c` or `--config_path` flag, or `CONFIG_PATH` env variable.
Rest assured, if your file is not well formatted, explicit validation errors will let you know as you run Chomp.

For an in-depth understanding of Chomp's configuration, please refer to its [yamale](https://github.com/23andMe/Yamale) validation schema: [./src/config-schema.yml](./src/config-schema.yml) and [./src/model.py](./src/model.py).


#### Base Template

The ingesters config file defines a list of all known resources (schemas), and specific attributes to configure their ingestion.

```yaml
scrapper: []
http_api:
  - name: ExampleIngester          # unique resource name (mapped to db table)
    resource_type: timeseries       # defaults to time indexing
    target: http://example.com/api  # ingester target
    interval: m1                    # ingestion interval
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

#### Generic Ingester Attributes

- **resource_type:** Ingestion/indexing type, any of `timeseries` or `value` (inplace values, FIFO)
- **target:** Resource target - eg. URL, contract address.
- **selector:** Field query/selector.
- **fields:** Defines the data fields to ingest.
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
| HTTP API | ✔️ | ❌ | ❌ |
| WS API | ✔️ | ❌ | ❌ |
| SQL API | partial | ✔️ | ❌ |
| GraphQL API | ❌ | ✔️ | ✔️ |
| **___ Ingesters ___** |
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

#### Ponder.sh vs Chomp

- **Hosting/Management:** Ponder is primarily self-hosted, just like Chomp.
- **Deployment Size:** Both Ponder's and Chomp's codebases are easy to comprehend, extend, and not very resource intensive for small schemas.
- **TheGraph Migration:** Ponder's migration guide and historical re-indexing capabilities make it a low-effort solution for teams already using The Graph. Chomp's not there yet, not being GraphQL native.
- **APIs:** Offers GraphQL and SQL APIs, but lacks websocket support for live streaming, dashboard/schema explorer. Chomp [already has](./src/server) or is experimenting with all, and provides a [production-ready, highly configurable rate-limiter](./src/server/middlewares/limiter.py).
- **Storage Back-end:** Supports SQLite and Postgres as only back-ends, neither are ideal for time-series storage out of the box. Chomp makes it possible for anyone to add a database adapter in minutes in [./src/adapters](./src/adapters)
- **Web2/Web3 Compatibility:** Limited to Web3 data ingestion. Chomp is Web2 friendly, enabling hybrid data setups to be deployed in minutes, [check it out](./examples/diverse.yml).

#### The Graph vs Chomp

- **Hosting/Management:** The Graph subgraphs generally run onto [the Hosted Service](https://thegraph.com/docs/en/deploying/hosted-service/), decentralized and managed, this allows for low-maintenance setups (at a cost). The initial learning curve for using The Graph is however very steep, where Chomp focuses on low-code, low-config setups.
- **Deployment Size:** Unlike Chomp, The Graph's stack make is tedious and resource-intensive to self-host, and its strict architecture makes it complicated for anyone to extend.
- **Documentation:** The Graph's maturity is reflected in its [comprehensive documentation](https://thegraph.com/docs/en/) and community.
- **APIs:** The Graph provides a great, [user friendly dashboard](https://thegraph.com/hosted-service/dashboard), as well as a time-proven, distributed GraphQL API. Chomp focuses on higher performance http and websocket setups, capable of real time data streaming.
- **Web2/Web3 Compatibility:** The Graph is currently limited to Web3 (EVMs and Solana) data ingestion, but fine grained: function calls/events/block mint hooks are all available. Chomp does not currently offer a block indexer as not prioritized, however allows for Web2 data ingestion.

## Contributing

Contributions are much welcome!

Chomp is currently mostly an multimodal ingester, and lacks:
- [GraphQL](https://graphql.org/) adapter - expose stored data similarly to [The Graph](https://thegraph.com/) using [Strawberry](https://strawberry.rocks/)
- UI to explore running nodes, configure ingesters configurations, monitor data feeds
- Adapters - currently [TDengine](https://tdengine.com/) was our focus for performance and stability purposes, but new database adapters can very easily be added to [./src/adapters](./src/adapters), we are already looking at [Timescale](https://www.timescale.com/), [Influx](https://www.influxdata.com/), [kdb/kx](https://kx.com/) and others
- Performance profiling and optimization (better IO, threading, transformers, or even a [Rust](https://www.rust-lang.org/) port)

## License
This project is licensed under the MIT License, use at will. ❤️
