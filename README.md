# TileStream

A small distributed image processing pipeline. You upload a large image, a master node cuts it into tiles and pushes them to Kafka, two workers process the tiles in parallel, and the master stitches everything back into one image. There is a web page for uploading and a dashboard that shows which workers are alive, based on their heartbeat messages.

I built this to learn how Kafka consumer groups, partitions and offsets work in practice, not to beat a single machine at image filters (see the benchmark section).

## How it works

```
Browser -> Master (FastAPI) -> Kafka "tasks" -> worker-1
                |                          \-> worker-2
                |<------ Kafka "results" <----/
                |<------ Kafka "heartbeats" <-/
```

- The master checks the upload (minimum 1024x1024), splits it into tiles of at least 512x512 and publishes them to the `tasks` topic, spread round-robin over 2 partitions.
- Both workers are in one consumer group, so Kafka gives each worker one partition.
- A worker processes a tile with OpenCV and publishes it to `results`. It commits its offset only after the result is sent, so a crashed worker's tile gets picked up again by the other one. The master ignores duplicates.
- Every worker sends a heartbeat every 2 seconds (status, partitions, current tile, CPU, RAM). The dashboard marks a worker offline after 6 seconds of silence.
- Job and tile metadata is kept in SQLite. Processed tiles are saved to disk and stitched once all of them have arrived.
- Each tile is sent with a 16 px border that is cropped off after processing. Without it, blur and edge filters leave visible seams. With it, the stitched result is pixel-identical to processing the whole image at once.

## Stack

Python 3.12, Apache Kafka 3.8 (Docker, KRaft mode, single broker), confluent-kafka, FastAPI, OpenCV, NumPy, SQLite, plain JavaScript for the dashboard.

Filters: grayscale, blur, sharpen, edge detection (Sobel), sepia, invert, bilateral denoise.

## Requirements

- Python 3.10 to 3.12
- Docker Desktop
- Windows (the .bat files are for Windows, the Python code itself is not OS specific)

## Setup

```
setup.bat
docker compose up -d
.venv\Scripts\python.exe -m scripts.create_topics
```

The last command creates the `tasks`, `results` and `heartbeats` topics and prints their partitions.

## Running

Open three terminals in the project folder:

```
run_master.bat
run_worker1.bat
run_worker2.bat
```

Then go to http://localhost:8000, pick an image and a filter, and hit Process. If you need test images, run `python -m tools.make_sample`.

To check how partitions are split between the workers:

```
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group image-workers
```

## Testing a worker crash

Start the slow workers instead (`run_worker1_slow.bat`, `run_worker2_slow.bat`, they add 400 ms per tile), upload a 4096x4096 image and close one worker's terminal window while the job is running. The dashboard shows it as offline, Kafka reassigns its partition after about 10 seconds, and the remaining worker finishes the job. Closing with Ctrl+C is a clean shutdown, so use the window close button for a real crash.

## Tools

- `python -m tools.verify_tiling` checks that tiled output matches single-pass output for every filter.
- `python -m tools.benchmark` runs timing tests. Examples:

```
python -m tools.benchmark --baseline --size 4096 --op denoise
python -m tools.benchmark --workers 2 --size 4096 --op denoise
python -m tools.benchmark --summary
```

For the `--workers` runs, the label is just what you tell it, so make sure the right number of workers is actually running.

## Benchmark results

4096x4096 image, bilateral denoise, 64 tiles, 3 runs each, everything on one laptop:

| Setup | Mean time |
|---|---|
| Single process, 1 thread, no Kafka | 4.32 s |
| 1 worker | 27.64 s |
| 2 workers | 22.37 s |

Going from 1 to 2 workers gave about a 1.24x speedup, and tiles were split evenly (32 each). But the whole pipeline was slower than just running the filter in one process. On a single machine the splitting (around 7 s) and stitching (around 4 s) in the master run serially, tiles are PNG encoded and decoded on every hop, and the master, workers and Docker all share the same CPU. I haven't profiled this properly, so that is my best explanation rather than a measured one. A real gain would need heavier per-tile work and workers on separate machines.

## Project layout

```
master/    FastAPI app, tiling, Kafka producer/consumers, SQLite
worker/    Kafka consumer, OpenCV processing, heartbeats
common/    config, Kafka helpers, image operations
scripts/   topic creation
tools/     benchmark, tiling check, sample image generator
```

## Configuration

Environment variables: `KAFKA_BOOTSTRAP` (default `localhost:9092`), `TILE_SIZE` (minimum 512), `WORKER_ARTIFICIAL_DELAY_MS`, `WORKER_CV_THREADS` (default 1).

To run workers on other machines, point `KAFKA_BOOTSTRAP` at the broker's IP and change `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` to the same IP.

## Known limitations

- Single Kafka broker, replication factor 1, so no broker fault tolerance.
- Only two workers are tested. The partition count is fixed at 2, so extra workers would sit idle unless you add partitions.
- The master keeps no queue of its own. If it is down, results wait in Kafka and get processed when it restarts.
- No authentication on the web page.
