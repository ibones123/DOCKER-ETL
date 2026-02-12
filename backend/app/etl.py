import os
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List

import requests


DEFAULT_DB_PATH = os.getenv("DB_PATH", "/data/app.db")
COUNTRIES_API_URL = os.getenv("COUNTRIES_API_URL", "https://restcountries.com/v3.1/all?fields=name,cca2,region,population,capital")


def _get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    conn = _get_conn(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cca2 TEXT NOT NULL,
            name TEXT NOT NULL,
            region TEXT,
            population INTEGER,
            capital TEXT,
            loaded_at TEXT NOT NULL,
            UNIQUE(cca2)
        );
        """)
        conn.commit()
    finally:
        conn.close()


def extract() -> List[Dict]:
    r = requests.get(COUNTRIES_API_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected API response format (expected a list).")
    return data


def transform(raw: List[Dict]) -> List[Dict]:
    transformed: List[Dict] = []
    loaded_at = datetime.now(timezone.utc).isoformat()

    for item in raw:
        cca2 = (item.get("cca2") or "").strip()
        name = (((item.get("name") or {}).get("common")) or "").strip()

        region = (item.get("region") or "").strip() or None
        population = item.get("population", None)
        capital_list = item.get("capital") or []
        capital = None
        if isinstance(capital_list, list) and len(capital_list) > 0:
            capital = str(capital_list[0]).strip() or None

        if not cca2 or not name:
            continue

        # population sometimes missing or not int -> normalize
        try:
            population_int = int(population) if population is not None else None
        except Exception:
            population_int = None

        transformed.append({
            "cca2": cca2,
            "name": name,
            "region": region,
            "population": population_int,
            "capital": capital,
            "loaded_at": loaded_at,
        })

    return transformed


def load(rows: List[Dict], db_path: str = DEFAULT_DB_PATH) -> Dict:
    init_db(db_path)
    conn = _get_conn(db_path)
    inserted = 0
    updated = 0

    try:
        cur = conn.cursor()
        for row in rows:
            # Upsert by cca2
            cur.execute("""
            INSERT INTO countries (cca2, name, region, population, capital, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(cca2) DO UPDATE SET
                name=excluded.name,
                region=excluded.region,
                population=excluded.population,
                capital=excluded.capital,
                loaded_at=excluded.loaded_at
            """, (
                row["cca2"],
                row["name"],
                row["region"],
                row["population"],
                row["capital"],
                row["loaded_at"],
            ))

            # sqlite doesn't give "insert vs update" reliably here; we approximate:
            # If lastrowid changed and rowcount==1 could still be update. We'll compute counts later.
        conn.commit()

        # Count total rows in table as a simple metric
        cur.execute("SELECT COUNT(*) as c FROM countries;")
        total = int(cur.fetchone()["c"])

    finally:
        conn.close()

    return {
        "status": "ok",
        "message": "ETL completed",
        "rows_processed": len(rows),
        "total_rows_in_db": total,
    }


def run_etl(db_path: str = DEFAULT_DB_PATH) -> Dict:
    raw = extract()
    rows = transform(raw)
    return load(rows, db_path=db_path)


def get_all_data(db_path: str = DEFAULT_DB_PATH, limit: int = 200, offset: int = 0) -> Dict:
    init_db(db_path)
    conn = _get_conn(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT cca2, name, region, population, capital, loaded_at
            FROM countries
            ORDER BY name ASC
            LIMIT ? OFFSET ?;
        """, (limit, offset))
        items = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT COUNT(*) as c FROM countries;")
        total = int(cur.fetchone()["c"])

        return {"total": total, "limit": limit, "offset": offset, "items": items}
    finally:
        conn.close()

