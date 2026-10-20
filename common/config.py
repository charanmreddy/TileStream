"""Central configuration. Override anything with environment variables."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Kafka
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_TASKS = "tasks"
TOPIC_RESULTS = "results"
TOPIC_HEARTBEATS = "heartbeats"
TASKS_PARTITIONS = int(os.getenv("TASKS_PARTITIONS", "2"))   # = number of workers
WORKER_GROUP = "image-workers"
MAX_MESSAGE_BYTES = 10 * 1024 * 1024                          # 10 MB per tile message

# Image / tiling rules from the project brief
MIN_IMAGE_SIDE = 1024                                         # min image = 1024x1024
TILE_SIZE = max(512, int(os.getenv("TILE_SIZE", "512")))      # min tile = 512x512
HALO = 16   # extra border pixels sent with each tile so blur/edge filters leave no seams

# Heartbeats
HEARTBEAT_INTERVAL_S = float(os.getenv("HEARTBEAT_INTERVAL_S", "2"))
HEARTBEAT_TIMEOUT_S = float(os.getenv("HEARTBEAT_TIMEOUT_S", "6"))   # silent this long = offline

# Storage (SQLite metadata + image files)
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", str(BASE_DIR / "storage")))
DB_PATH = STORAGE_DIR / "pipeline.db"
