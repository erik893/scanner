import os
from fastapi import FastAPI
from google.cloud import firestore

app = FastAPI()
db = firestore.Client()

@app.get("/")
def root():
    return {"ok": True, "service": "drive-scanner"}

@app.post("/scan")
def scan():
    # ✅ Firestore Test Write
    db.collection("state").document("hello").set({
        "ok": True,
        "message": "firestore write works"
    })
    return {"ok": True, "message": "wrote to firestore: state/hello"}
