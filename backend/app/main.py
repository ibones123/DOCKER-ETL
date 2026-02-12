import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .etl import run_etl, get_all_data, init_db, DEFAULT_DB_PATH

app = FastAPI(title="ETL API", version="1.0.0")

# CORS (dev friendly). In prod, restrict origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)


@app.on_event("startup")
def startup():
    # Ensure DB exists at boot
    init_db(DB_PATH)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/etl/run")
def etl_run():
    try:
        return run_etl(DB_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data")
def data(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    try:
        return get_all_data(DB_PATH, limit=limit, offset=offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

