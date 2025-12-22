import os
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "drive-scanner", "port": os.environ.get("PORT")}

@app.post("/scan")
def scan():
    # Dummy – nur zum Testen ob Deploy/Port passt
    return {"ok": True, "message": "scan endpoint works"}
