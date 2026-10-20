"""Measure the pipeline. Master + workers must be running.

Examples (run from project root):
  python -m tools.benchmark --baseline --size 4096 --op denoise       # no Kafka, 1 thread
  python -m tools.benchmark --workers 1 --size 4096 --op denoise      # only worker-1 running
  python -m tools.benchmark --workers 2 --size 4096 --op denoise      # both workers running
  python -m tools.benchmark --summary                                 # speedup table
"""
import argparse
import csv
import statistics
import time
from collections import defaultdict
from pathlib import Path

import cv2
import requests

from common import operations
from tools.make_sample import make_image

OUT = Path(__file__).resolve().parent.parent / "benchmarks"
CSV = OUT / "results.csv"
FIELDS = ["timestamp", "label", "size", "op", "tiles", "elapsed_s", "tiles_per_s", "per_worker"]


def save_row(row):
    OUT.mkdir(exist_ok=True)
    new = not CSV.exists()
    with CSV.open("a", newline="") as f:
        w = csv.DictWriter(f, FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)


def run_baseline(size, op, runs):
    cv2.setNumThreads(1)
    img = make_image(size)
    for i in range(runs):
        t0 = time.perf_counter()
        operations.apply(op, img)
        dt = time.perf_counter() - t0
        tiles = (size // 512) ** 2
        print(f"[baseline run {i+1}] {dt:.2f}s (single process, 1 thread, no Kafka)")
        save_row({"timestamp": int(time.time()), "label": "baseline", "size": size, "op": op,
                  "tiles": tiles, "elapsed_s": round(dt, 3), "tiles_per_s": round(tiles / dt, 2),
                  "per_worker": "-"})


def run_pipeline(url, size, op, runs, label):
    ok, buf = cv2.imencode(".jpg", make_image(size), [cv2.IMWRITE_JPEG_QUALITY, 92])
    data = buf.tobytes()
    for i in range(runs):
        t0 = time.perf_counter()
        r = requests.post(f"{url}/api/jobs", files={"file": ("bench.jpg", data, "image/jpeg")},
                          data={"operation": op}, timeout=120)
        r.raise_for_status()
        job_id = r.json()["job_id"]
        while True:
            j = requests.get(f"{url}/api/jobs/{job_id}", timeout=30).json()
            if j["status"] in ("done", "failed"):
                break
            time.sleep(0.1)
        wall = time.perf_counter() - t0
        if j["status"] == "failed":
            print("FAILED:", j.get("error"))
            return
        per = defaultdict(int)
        for t in j["tiles"]:
            per[t["worker_id"]] += 1
        per_s = " ".join(f"{k}={v}" for k, v in sorted(per.items()))
        print(f"[{label} worker(s) run {i+1}] job {job_id}: {wall:.2f}s wall, "
              f"{j['tile_count']} tiles, {j['tile_count']/wall:.2f} tiles/s, split {j['split_ms']:.0f} ms, "
              f"stitch {j['stitch_ms']:.0f} ms, tiles per worker: {per_s}")
        save_row({"timestamp": int(time.time()), "label": label, "size": size, "op": op,
                  "tiles": j["tile_count"], "elapsed_s": round(wall, 3),
                  "tiles_per_s": round(j["tile_count"] / wall, 2), "per_worker": per_s})


def summary():
    if not CSV.exists():
        print("No results yet.")
        return
    groups = defaultdict(lambda: defaultdict(list))
    with CSV.open() as f:
        for r in csv.DictReader(f):
            groups[(r["size"], r["op"])][r["label"]].append(float(r["elapsed_s"]))
    for (size, op), labels in groups.items():
        print(f"\n== {size}x{size}, op={op} ==")
        ref = statistics.mean(labels["1"]) if "1" in labels else None
        for label in sorted(labels):
            m = statistics.mean(labels[label])
            sp = f"  speedup vs 1 worker: {ref/m:.2f}x" if ref and label not in ("1",) else ""
            print(f"  {label:<9} mean {m:7.2f}s over {len(labels[label])} run(s){sp}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--size", type=int, default=4096)
    ap.add_argument("--op", default="denoise", choices=list(operations.OPERATIONS))
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--workers", default="2", help="label = how many workers are running right now")
    ap.add_argument("--baseline", action="store_true")
    ap.add_argument("--summary", action="store_true")
    a = ap.parse_args()
    if a.summary:
        summary()
    elif a.baseline:
        run_baseline(a.size, a.op, a.runs)
    else:
        run_pipeline(a.url, a.size, a.op, a.runs, a.workers)
