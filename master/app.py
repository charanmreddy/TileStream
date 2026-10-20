"""Node 1 - Client UI + Master. Run with:  python -m uvicorn master.app:app --port 8000"""
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse

from common import config, operations
from . import db
from .service import master

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app):
    master.start()
    yield
    master.stop()


app = FastAPI(title="Distributed Image Processing Pipeline", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/operations")
def get_operations():
    return operations.OPERATIONS


@app.post("/api/jobs")
def create_job(file: UploadFile = File(...), operation: str = Form("grayscale")):
    data = file.file.read()
    try:
        job_id = master.submit_job(file.filename or "upload", data, operation)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"job_id": job_id}


def _decorate(job):
    end = job["finished_at"] or time.time()
    job["elapsed_s"] = round(end - job["created_at"], 2)
    return job


@app.get("/api/jobs")
def list_jobs():
    return [_decorate(j) for j in db.list_jobs()]


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    job = _decorate(job)
    job["tiles"] = db.get_tiles(job_id)
    return job


def _job_file(job_id: str, name: str):
    p = config.STORAGE_DIR / "jobs" / job_id / name
    if not p.exists():
        raise HTTPException(404, f"{name} not available (yet)")
    return p


@app.get("/api/jobs/{job_id}/original")
def original(job_id: str):
    return FileResponse(_job_file(job_id, "original.png"), media_type="image/png")


@app.get("/api/jobs/{job_id}/result")
def result(job_id: str):
    return FileResponse(_job_file(job_id, "result.png"), media_type="image/png")


@app.get("/api/jobs/{job_id}/result/download")
def result_download(job_id: str):
    return FileResponse(_job_file(job_id, "result.png"), media_type="image/png",
                        filename=f"processed_{job_id}.png")


@app.get("/api/workers")
def workers():
    stats = db.worker_stats()
    out = []
    for w in master.workers.snapshot():
        s = stats.get(w["worker_id"], {"tiles": 0, "avg_ms": 0})
        out.append({**w, "tiles_done_total": s["tiles"], "avg_tile_ms": s["avg_ms"]})
    return out


@app.get("/api/status")
def status():
    ws = master.workers.snapshot()
    return {
        "kafka_ok": master.check_kafka(),
        "bootstrap": config.BOOTSTRAP,
        "active_workers": sum(1 for w in ws if w["online"]),
        "workers_seen": len(ws),
        **db.summary(),
    }
