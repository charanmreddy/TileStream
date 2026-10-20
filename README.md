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

## Project layout

```
master/    FastAPI app, tiling, Kafka producer/consumers, SQLite
worker/    Kafka consumer, OpenCV processing, heartbeats
common/    config, Kafka helpers, image operations
scripts/   topic creation
tools/     benchmark, tiling check, sample image generator
batch/     all the .bat files to create setup and run workers
```

## Configuration

Environment variables: `KAFKA_BOOTSTRAP` (default `localhost:9092`), `TILE_SIZE` (minimum 512), `WORKER_ARTIFICIAL_DELAY_MS`, `WORKER_CV_THREADS` (default 1).

To run workers on other machines, point `KAFKA_BOOTSTRAP` at the broker's IP and change `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` to the same IP.
