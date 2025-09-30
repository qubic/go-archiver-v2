# The qubic archiver service (version 2)

> The archiver service's purpose is to create archives of epoch data and make the data available for further processing.

## High level description:

The archive system consists of two services:
- `qubic-archiver-v2` - the archiver processor and HTTP server that provides rpc endpoints to query the archiver
- `qubic-nodes` - a service responsible with providing information regarding reliable nodes and the max tick of the
  network

## IMPORTANT

> [!WARNING]  
> The qubic archiver v2 and its data is **INCOMPATIBLE** with the old version.
> Certain indices have been removed and the data is split into single epochs for easier archivation and service
> operation. 
> The archiver **DOES NOT** migrate the database to the new format by itself, and **MAY BREAK** your existing 
> information, if not migrated correctly. Please use the migration tool for converting the old multi-epoch archive 
> into several new archives (one per epoch).

## Build

To build from source execute `go build` in the root level directory.

## Run

To run the built application execute `./go-archiver-v2` in the root level directory and provide the required configuration
parameters.

Alternatively you can run with docker. See the docker compose example and adapt to your needs. It is important to have
a correctly configured `qubic-nodes` service up and running to be able to start the archiving. Make sure to configure
valid and reachable peers.

On startup the application will load the databases stored in the data folder. Defaults to `./archive-data`. You need
one subfolder per epoch with the corresponding epoch number.

## Configuration

```terminaloutput
    --pool-idle-timeout=15s
    --pool-initial-cap=5
    --pool-max-cap=30
    --pool-max-idle=20
    --pool-node-fetcher-timeout=2s
    --pool-node-fetcher-url=http://127.0.0.1:8080/status
    --qubic-arbitrator-identity=AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ
    --qubic-enable-tx-status-addon=true
    --qubic-node-port=21841
    --qubic-process-tick-timeout=5s
    --server-grpc-host=0.0.0.0:8001
    --server-http-host=0.0.0.0:8000
    --server-node-sync-threshold=10
    --server-profiling-host=0.0.0.0:8002
    --server-read-timeout=5s
    --server-shutdown-timeout=5s
    --server-write-timeout=5s
    --store-open-epochs-count=10
    --store-storage-folder=archive-data
```

## API endpoints

See the openapi endpoint documentation.

## References

The new archiver works quite similar to the old version in principle. It has other endpoints and less search
capabilities but some information there might be useful: see the [go-archiver](https://github.com/qubic/go-archiver) 
repository for the old documentation.