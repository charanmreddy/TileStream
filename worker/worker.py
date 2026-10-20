"""Nodes 3 & 4 - Worker. Run:  python -m worker.worker --id worker-1

Consumes tiles from `tasks` (as part of one consumer group), processes them with
OpenCV, publishes results to `results`, and sends a heartbeat every few seconds."""
import argparse
import json
import logging
import os
import threading
import time

import cv2
import psutil

from common import config, operations
from common import kafka_utils as ku

log = logging.getLogger("worker")


class State:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.lock = threading.Lock()
        self.status = "starting"
        self.current = None
        self.partitions = []
        self.processed = 0
        self.errors = 0
        self.last_tile_ms = 0
        self.started = time.time()

    def set(self, **kw):
        with self.lock:
            for k, v in kw.items():
                setattr(self, k, v)


def heartbeat_loop(producer, state, stop):
    proc = psutil.Process()
    ncpu = psutil.cpu_count() or 1
    proc.cpu_percent(None)
    while not stop.is_set():
        send_heartbeat(producer, state, proc, ncpu)
        stop.wait(config.HEARTBEAT_INTERVAL_S)


def send_heartbeat(producer, state, proc=None, ncpu=1, status=None):
    proc = proc or psutil.Process()
    with state.lock:
        hb = {
            "worker_id": state.worker_id,
            "ts": time.time(),
            "status": status or state.status,
            "current_task": state.current,
            "partitions": list(state.partitions),
            "tiles_processed": state.processed,
            "errors": state.errors,
            "last_tile_ms": state.last_tile_ms,
            "uptime_s": round(time.time() - state.started),
            "cpu_percent": round(proc.cpu_percent(None) / ncpu, 1),
            "mem_mb": round(proc.memory_info().rss / 1e6),
        }
    producer.produce(config.TOPIC_HEARTBEATS, key=state.worker_id, value=json.dumps(hb))
    producer.poll(0)


def process_message(msg, producer, state, delay_ms):
    h = ku.headers_to_dict(msg.headers())
    job_id, idx = h["job_id"], h["tile_idx"]
    state.set(status="busy", current=f"{job_id}:{idx}")
    t0 = time.perf_counter()
    out_headers = {"job_id": job_id, "tile_idx": idx, "worker_id": state.worker_id}
    try:
        png = operations.process_tile_bytes(
            h["op"], msg.value(), int(h["px"]), int(h["py"]), int(h["cw"]), int(h["ch"]))
        if delay_ms:
            time.sleep(delay_ms / 1000.0)       # artificial slow-down for demos
        out_headers["proc_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        value = png
        state.set(processed=state.processed + 1, last_tile_ms=out_headers["proc_ms"])
    except Exception as e:                      # report the failure instead of looping forever
        log.exception("Tile %s:%s failed", job_id, idx)
        out_headers["error"] = str(e)[:200]
        value = b""
        state.set(errors=state.errors + 1)
    producer.produce(config.TOPIC_RESULTS, key=f"{job_id}:{idx}", value=value,
                     headers=ku.dict_to_headers(out_headers),
                     partition=int(msg.partition()) % max(1, config.TASKS_PARTITIONS))
    producer.flush(30)
    log.info("Processed tile %s:%s from partition %s in %s ms", job_id, idx, msg.partition(),
             out_headers.get("proc_ms", "ERR"))
    state.set(status="idle", current=None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.getenv("WORKER_ID", f"worker-{os.getpid()}"))
    ap.add_argument("--delay-ms", type=int, default=int(os.getenv("WORKER_ARTIFICIAL_DELAY_MS", "0")),
                    help="extra sleep per tile, to make jobs slow enough to watch/kill a worker")
    ap.add_argument("--cv-threads", type=int, default=int(os.getenv("WORKER_CV_THREADS", "1")),
                    help="OpenCV threads per worker (1 = behave like a small independent node)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s %(levelname)s [{args.id}] %(message)s")
    cv2.setNumThreads(args.cv_threads)

    state = State(args.id)
    producer = ku.make_producer()
    consumer = ku.make_consumer(config.WORKER_GROUP)

    def on_assign(_c, parts):
        state.set(partitions=sorted(p.partition for p in parts))
        log.info("Partitions assigned: %s", state.partitions)

    def on_revoke(_c, parts):
        log.info("Partitions revoked: %s", sorted(p.partition for p in parts))
        state.set(partitions=[])

    consumer.subscribe([config.TOPIC_TASKS], on_assign=on_assign, on_revoke=on_revoke)

    stop = threading.Event()
    threading.Thread(target=heartbeat_loop, args=(producer, state, stop), daemon=True).start()
    state.set(status="idle")
    log.info("Worker started (group=%s, delay=%d ms, cv-threads=%d). Ctrl+C to stop.",
             config.WORKER_GROUP, args.delay_ms, args.cv_threads)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer: %s", msg.error())
                continue
            process_message(msg, producer, state, args.delay_ms)
            consumer.commit(message=msg, asynchronous=False)   # commit only after result is sent
    except KeyboardInterrupt:
        log.info("Stopping...")
    finally:
        stop.set()
        state.set(status="offline", partitions=[], current=None)
        try:
            send_heartbeat(producer, state, status="offline")
            producer.flush(5)
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
