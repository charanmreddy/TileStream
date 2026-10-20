"""Master logic: split images, publish tiles, collect results, track workers."""
import json
import logging
import threading
import time
import uuid

import cv2

from common import config, operations
from common import kafka_utils as ku
from . import db, tiling

log = logging.getLogger("master")


class WorkerRegistry:
    """Latest heartbeat of every worker we have ever heard from."""

    def __init__(self):
        self._d = {}
        self._lock = threading.Lock()

    def update(self, hb):
        hb["last_seen"] = time.time()          # master's clock, so no clock-skew problems
        with self._lock:
            self._d[hb["worker_id"]] = hb

    def snapshot(self):
        now = time.time()
        out = []
        with self._lock:
            for w in self._d.values():
                age = now - w["last_seen"]
                online = age <= config.HEARTBEAT_TIMEOUT_S and w.get("status") != "offline"
                out.append({**w, "age_s": round(age, 1), "online": online})
        return sorted(out, key=lambda w: w["worker_id"])


class Master:
    def __init__(self):
        self.producer = None
        self.workers = WorkerRegistry()
        self._stop = threading.Event()
        self._kafka_cache = (0.0, False)

    # ---------- lifecycle ----------
    def start(self):
        db.init()
        try:
            created = ku.ensure_topics()
            log.info("Kafka reachable at %s (created topics: %s)", config.BOOTSTRAP, created or "none")
        except Exception as e:
            log.warning("Could not reach Kafka at %s yet: %s", config.BOOTSTRAP, e)
        self.producer = ku.make_producer()
        for target in (self._results_loop, self._heartbeat_loop):
            threading.Thread(target=target, daemon=True).start()

    def stop(self):
        self._stop.set()

    def check_kafka(self):
        ts, ok = self._kafka_cache
        if time.time() - ts < 5:
            return ok
        try:
            self.producer.list_topics(timeout=2)
            ok = True
        except Exception:
            ok = False
        self._kafka_cache = (time.time(), ok)
        return ok

    # ---------- submitting a job ----------
    def submit_job(self, filename, data, op):
        if op not in operations.OPERATIONS:
            raise ValueError(f"Unknown operation '{op}'")
        img = operations.decode(data)
        if img is None:
            raise ValueError("That file is not a readable image.")
        h, w = img.shape[:2]
        if w < config.MIN_IMAGE_SIDE or h < config.MIN_IMAGE_SIDE:
            raise ValueError(f"Image is {w}x{h}. Minimum allowed size is "
                             f"{config.MIN_IMAGE_SIDE}x{config.MIN_IMAGE_SIDE}.")
        if not self.check_kafka():
            raise ConnectionError("Kafka broker is not reachable. Is Docker/Kafka running?")

        t0 = time.perf_counter()
        job_id = uuid.uuid4().hex[:8]
        job_dir = config.STORAGE_DIR / "jobs" / job_id
        (job_dir / "tiles").mkdir(parents=True, exist_ok=True)
        cv2.imwrite(str(job_dir / "original.png"), img, [cv2.IMWRITE_PNG_COMPRESSION, 1])

        grid = tiling.compute_grid(w, h)
        n_parts = self._partition_count()
        # Build all task messages first so we can record "split_ms" before publishing.
        tasks = []
        for idx, (x, y, tw, th) in enumerate(grid):
            payload, meta = tiling.make_task(img, x, y, tw, th)
            tasks.append((idx, payload, meta))
        split_ms = (time.perf_counter() - t0) * 1000
        db.create_job(job_id, filename, op, w, h, grid, split_ms, n_parts)

        errors = []

        def on_delivery(err, _msg):
            if err is not None:
                errors.append(str(err))

        for idx, payload, meta in tasks:
            headers = ku.dict_to_headers({"job_id": job_id, "tile_idx": idx, "op": op,
                                          "total": len(tasks), **meta})
            while True:
                try:
                    self.producer.produce(
                        config.TOPIC_TASKS, key=f"{job_id}:{idx}", value=payload,
                        headers=headers, partition=idx % n_parts,   # round-robin over partitions
                        on_delivery=on_delivery)
                    break
                except BufferError:
                    self.producer.poll(0.5)
            self.producer.poll(0)
        self.producer.flush(30)
        if errors:
            db.fail_job(job_id, "Kafka delivery failed: " + errors[0])
        log.info("Job %s: %dx%d, op=%s, %d tiles published (split %.0f ms)",
                 job_id, w, h, op, len(tasks), split_ms)
        return job_id

    def _partition_count(self):
        try:
            md = self.producer.list_topics(config.TOPIC_TASKS, timeout=5)
            return max(1, len(md.topics[config.TOPIC_TASKS].partitions))
        except Exception:
            return config.TASKS_PARTITIONS

    # ---------- results consumer ----------
    def _results_loop(self):
        consumer = ku.make_consumer("master-results")
        consumer.subscribe([config.TOPIC_RESULTS])
        log.info("Results consumer started")
        while not self._stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("results consumer: %s", msg.error())
                continue
            try:
                self._handle_result(msg)
            except Exception:
                log.exception("Failed to handle a result message")
            consumer.commit(message=msg, asynchronous=False)
        consumer.close()

    def _handle_result(self, msg):
        h = ku.headers_to_dict(msg.headers())
        job_id, idx = h["job_id"], int(h["tile_idx"])
        job = db.get_job(job_id)
        if not job or job["status"] in ("done", "failed"):
            return                                  # stale message from an old job
        if h.get("error"):
            db.fail_job(job_id, f"tile {idx} failed on {h.get('worker_id')}: {h['error']}")
            return
        tile_path = config.STORAGE_DIR / "jobs" / job_id / "tiles" / f"{idx:04d}.png"
        tile_path.write_bytes(msg.value())
        is_new, should_stitch = db.mark_tile_done(
            job_id, idx, h.get("worker_id", "?"), float(h.get("proc_ms", 0)))
        if is_new and should_stitch:
            self._finalize(job)

    def _finalize(self, job):
        t0 = time.perf_counter()
        job_id = job["id"]
        try:
            tiles = [(t["x"], t["y"], t["w"], t["h"],
                      config.STORAGE_DIR / "jobs" / job_id / "tiles" / f"{t['idx']:04d}.png")
                     for t in db.get_tiles(job_id)]
            full = tiling.stitch(job["width"], job["height"], tiles)
            cv2.imwrite(str(config.STORAGE_DIR / "jobs" / job_id / "result.png"), full)
            stitch_ms = (time.perf_counter() - t0) * 1000
            db.finish_job(job_id, stitch_ms)
            j = db.get_job(job_id)
            log.info("Job %s finished in %.2fs (stitch %.0f ms)", job_id,
                     j["finished_at"] - j["created_at"], stitch_ms)
        except Exception as e:
            log.exception("Stitching failed")
            db.fail_job(job_id, f"stitching failed: {e}")

    # ---------- heartbeat consumer ----------
    def _heartbeat_loop(self):
        group = f"master-heartbeats-{uuid.uuid4().hex[:6]}"
        consumer = ku.make_consumer(group, {"auto.offset.reset": "latest"})
        consumer.subscribe([config.TOPIC_HEARTBEATS])
        log.info("Heartbeat consumer started")
        while not self._stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                self.workers.update(json.loads(msg.value()))
            except Exception:
                log.warning("Bad heartbeat message ignored")
        consumer.close()


master = Master()
