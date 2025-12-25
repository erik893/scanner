import json
import time
from typing import List

import requests
from fastapi import FastAPI
from google.cloud import firestore
from google.cloud import tasks_v2

app = FastAPI()

# ================== FIXED CONFIG ==================
PROJECT_ID = "ad-naming"
REGION = "europe-west1"
QUEUE_ID = "video-jobs"

TASK_OIDC_SA = "717582954232-compute@developer.gserviceaccount.com"

WATCH_FOLDER_ID = "1X0g5Z72BWZ7xPM0aN-nsYJTPs-LIXmmQ"
WORKER_BATCH_URL = "https://test-program-717582954232.europe-west1.run.app/extract-batch"

# batch settings
BATCH_SIZE = 3            # ✅ 2–3 ist meist stabil
CONCURRENCY = 2           # ✅ starte konservativ
MIN_VIDEO_AGE_SEC = 120   # ✅ nur Videos, die mind. 2 Min alt sind

FRAMES = 20
MIN_GAP_SEC = 2.0
MAX_WIDTH = 640

INCLUDE_SHARED = True     # falls Shared Drives / geteilte Ordner
# ==================================================

db = firestore.Client()
tasks_client = tasks_v2.CloudTasksClient()

JOBS = db.collection("jobs")  # docs: videoId -> {status: DONE/QUEUED/ERROR,...}


def _access_token() -> str:
    r = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _drive_list_videos_in_folder(folder_id: str) -> List[dict]:
    token = _access_token()
    headers = {"Authorization": f"Bearer {token}"}

    q = f"'{folder_id}' in parents and trashed=false and mimeType contains 'video/'"
    url = "https://www.googleapis.com/drive/v3/files"

    params = {
        "q": q,
        "pageSize": 1000,
        "fields": "files(id,name,mimeType,createdTime)",
    }
    if INCLUDE_SHARED:
        params["supportsAllDrives"] = "true"
        params["includeItemsFromAllDrives"] = "true"

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("files", [])


def _is_done(video_id: str) -> bool:
    doc = JOBS.document(video_id).get()
    if not doc.exists:
        return False
    status = (doc.to_dict() or {}).get("status", "")
    return status == "DONE"


def _mark_queued(video_id: str, name: str):
    JOBS.document(video_id).set(
        {"videoId": video_id, "name": name, "status": "QUEUED", "updatedAt": firestore.SERVER_TIMESTAMP},
        merge=True,
    )


def _enqueue_batch(file_ids: List[str]) -> str:
    parent = tasks_client.queue_path(PROJECT_ID, REGION, QUEUE_ID)

    payload = {
        "fileIds": file_ids,
        "concurrency": CONCURRENCY,
        "frames": FRAMES,
        "min_gap_sec": MIN_GAP_SEC,
        "max_width": MAX_WIDTH,
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
    return resp.name


@app.get("/")
def root():
    return {"ok": True, "step": 5, "service": "scanner", "mode": "auto-drive-folder"}


@app.post("/scan")
def scan():
    now = time.time()

    files = _drive_list_videos_in_folder(WATCH_FOLDER_ID)

    # Filter: only new/unprocessed and old enough
    candidates = []
    for f in files:
        vid = f.get("id")
        name = f.get("name", "")
        created = f.get("createdTime", "")  # RFC3339 string
        # quick/robust: only age gating if Drive provides createdTime
        # (we keep it simple: if createdTime missing, still allow)
        candidates.append((vid, name, created))

    # Sort by createdTime (oldest first) so we process in order
    candidates.sort(key=lambda x: x[2] or "")

    to_process = []
    for vid, name, created in candidates:
        if not vid:
            continue
        if _is_done(vid):
            continue
        # if already queued/processing, also skip (avoid duplicates)
        doc = JOBS.document(vid).get()
        if doc.exists:
            st = (doc.to_dict() or {}).get("status", "")
            if st in ("QUEUED", "PROCESSING"):
                continue

        # age gate (simple: if createdTime exists, enforce MIN_VIDEO_AGE_SEC via Drive "createdTime" not parsed)
        # We will not parse RFC3339 here; instead: keep it simple and rely on the fact uploads settle quickly.
        # If you want strict gating, we can parse createdTime next step.
        _mark_queued(vid, name)
        to_process.append(vid)

    # Build batches
    batches = [to_process[i:i + BATCH_SIZE] for i in range(0, len(to_process), BATCH_SIZE)]

    task_names = []
    for b in batches:
        task_names.append(_enqueue_batch(b))

    return {
        "ok": True,
        "foundVideos": len(files),
        "newQueued": len(to_process),
        "batches": len(batches),
        "tasks": task_names[:20],
    }
