from fastapi import FastAPI
from google.cloud import firestore
from google.cloud import tasks_v2

app = FastAPI()

# ================== FESTE KONFIGURATION ==================

PROJECT_ID = "ad-naming"
REGION = "europe-west1"
QUEUE_ID = "video-jobs"

TASK_OIDC_SA = "717582954232-compute@developer.gserviceaccount.com"
SCANNER_BASE_URL = "https://scanner-717582954232.europe-west1.run.app"

# ========================================================

db = firestore.Client()
tasks_client = tasks_v2.CloudTasksClient()


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "drive-scanner",
        "step": 3
    }


@app.post("/task-test")
def task_test():
    # Wird vom Cloud Task aufgerufen
    db.collection("state").document("task_test").set({
        "ok": True,
        "message": "cloud task successfully reached /task-test"
    })
    return {"ok": True, "message": "task executed"}


@app.post("/scan")
def scan():
    # Firestore Test (aus Schritt 2)
    db.collection("state").document("hello").set({
        "ok": True,
        "message": "firestore write works"
    })

    # Cloud Task erstellen
    parent = tasks_client.queue_path(PROJECT_ID, REGION, QUEUE_ID)
    url = SCANNER_BASE_URL + "/task-test"

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": b"{}",
            "oidc_token": {
                "service_account_email": TASK_OIDC_SA
            },
        }
    }

    resp = tasks_client.create_task(
        request={"parent": parent, "task": task}
    )

    return {
        "ok": True,
        "message": "cloud task enqueued",
        "taskName": resp.name
    }
