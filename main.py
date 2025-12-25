import json
from fastapi import FastAPI
from pydantic import BaseModel, Field
from google.cloud import firestore
from google.cloud import tasks_v2

app = FastAPI()

# ================== FIXED CONFIG (hardcoded) ==================
PROJECT_ID = "ad-naming"
REGION = "europe-west1"
QUEUE_ID = "video-jobs"

# Cloud Tasks will call the Worker with an OIDC token from this SA:
TASK_OIDC_SA = "717582954232-compute@developer.gserviceaccount.com"

# Your Worker batch endpoint:
WORKER_BATCH_URL = "https://test-program-717582954232.europe-west1.run.app/extract-batch"

# Defaults (can be overridden by request body)
DEFAULT_CONCURRENCY = 2
DEFAULT_FRAMES = 20
DEFAULT_MIN_GAP_SEC = 2.0
DEFAULT_MAX_WIDTH = 640
# ===============================================================

db = firestore.Client()
tasks_client = tasks_v2.CloudTasksClient()


class ScanBatchReq(BaseModel):
    fileIds: list[str] = Field(..., min_length=1)
    concurrency: int = DEFAULT_CONCURRENCY
    frames: int = DEFAULT_FRAMES
    min_gap_sec: float = DEFAULT_MIN_GAP_SEC
    max_width: int = DEFAULT_MAX_WIDTH


@app.get("/")
def root():
    return {"ok": True, "service": "scanner", "step": 4, "mode": "extract-batch"}


@app.post("/scan")
def scan(req: ScanBatchReq):
    # sanitize
    file_ids = [x.strip() for x in req.fileIds if x and x.strip()]
    if not file_ids:
        return {"ok": False, "error": "fileIds is empty"}

    # 1) Write job entries (minimal tracking)
    for vid in file_ids:
        db.collection("jobs").document(vid).set(
            {"videoId": vid, "status": "QUEUED"},
            merge=True,
        )

    # 2) Create Cloud Task -> calls Worker /extract-batch
    parent = tasks_client.queue_path(PROJECT_ID, REGION, QUEUE_ID)

    payload = {
        "fileIds": file_ids,
        "concurrency": int(req.concurrency),
        "frames": int(req.frames),
        "min_gap_sec": float(req.min_gap_sec),
        "max_width": int(req.max_width),
    }

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": WORKER_BATCH_URL,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode("utf-8"),
            "oidc_token": {"service_account_email": TASK_OIDC_SA},
        }
    }

    resp = tasks_client.create_task(request={"parent": parent, "task": task})

    return {
        "ok": True,
        "message": "enqueued /extract-batch task",
        "count": len(file_ids),
        "payload": payload,
        "taskName": resp.name,
    }
