import os, json, requests
from fastapi import FastAPI
from google.cloud import firestore
from google.cloud import tasks_v2

app = FastAPI()

# ========= CONFIG (kommt später aus ENV) =========
PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ.get("REGION", "europe-west1")
QUEUE_ID = os.environ["QUEUE_ID"]

WATCH_FOLDER_ID = os.environ["WATCH_FOLDER_ID"]
WORKER_URL = os.environ["WORKER_URL"]
TASK_OIDC_SA = os.environ["TASK_OIDC_SA"]

db = firestore.Client()
tasks = tasks_v2.CloudTasksClient()

STATE_DOC = db.collection("state").document("drive_changes")
JOBS = db.collection("jobs")


def _access_token():
    r = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _drive_headers():
    return {"Authorization": f"Bearer {_access_token()}"}


@app.post("/scan")
def scan():
    # 1️⃣ Initial token holen (nur beim ersten Mal)
    state = STATE_DOC.get()
    if not state.exists:
        r = requests.get(
            "https://www.googleapis.com/drive/v3/changes/startPageToken",
            headers=_drive_headers(),
        )
        token = r.json()["startPageToken"]
        STATE_DOC.set({"pageToken": token})
        return {"ok": True, "message": "Initialized startPageToken"}

    page_token = state.to_dict()["pageToken"]

    # 2️⃣ Changes holen
    r = requests.get(
        "https://www.googleapis.com/drive/v3/changes",
        headers=_drive_headers(),
        params={
            "pageToken": page_token,
            "fields": "nextPageToken,newStartPageToken,changes(fileId,file(mimeType,parents,name))",
        },
    )
    data = r.json()

    enqueued = 0

    for ch in data.get("changes", []):
        f = ch.get("file")
        if not f:
            continue

        if not f.get("mimeType", "").startswith("video/"):
            continue

        if WATCH_FOLDER_ID not in (f.get("parents") or []):
            continue

        video_id = ch["fileId"]

        if JOBS.document(video_id).get().exists:
            continue

        # 3️⃣ Cloud Task erstellen
        parent = tasks.queue_path(PROJECT_ID, REGION, QUEUE_ID)

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": WORKER_URL,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"fileId": video_id}).encode(),
                "oidc_token": {"service_account_email": TASK_OIDC_SA},
            }
        }

        tasks.create_task(parent=parent, task=task)

        JOBS.document(video_id).set({
            "status": "QUEUED",
            "name": f.get("name"),
        })

        enqueued += 1

    # 4️⃣ Token weiterschieben
    STATE_DOC.set({
        "pageToken": data.get("newStartPageToken") or data.get("nextPageToken")
    })

    return {"ok": True, "enqueued": enqueued}
