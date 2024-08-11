# crivoe

Web scraper with extensions.

Dispatcher queue-based web scraping tool with HTTP API supporwing extensions with custom user-defined worker logic.

# Features

crivoe provides multiple features:
* Each task and Job has unique Id
* Each task and job supplied with it's own Status allowing user to track scraping progress
* Task and Job can store result metadata allowing user to fetch it on complete
  * For example, metadata can store response headers and other useful information
* Job scrape payload can be retrieved using reader
* Job result data download

# Usage

## Build

```bash

go build -o crivoe crivoe

```

## Run

```

$ ./crivoe -h
Usage of ./crivoe:
  -debug
        debug logging
  -host string
        server host (default "0.0.0.0")
  -log
        progress logging
  -memory
        memory mode
  -port int
        server port (default 8374)
  -storage string
        storage path (default "storage")

```

# API

Reference: [types.go](src/crivoe/api/types.go)

# TODO

* Handle Task `Maxretries`
* Parallel downloader worker
* Delete storage data on TTL
* Allow cancel tasks
* ~~List Tasks~~
* Split KVS for Tasks and for Jobs
