"""Load new JSON-lines files from SeaweedFS (S3) into raw.spotify_events in PostgreSQL.

Idempotent: processed files are tracked in raw.loaded_files and events are
de-duplicated by event_id.
"""
import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import Json, execute_values
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

BUCKET = os.getenv("S3_BUCKET", "spotify-data")
INTERVAL = 60  # seconds between loads when run with --watch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("loader")

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY", "any"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY", "any"),
    region_name="us-east-1",
)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.spotify_events (
    event_id        TEXT PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    track_id        TEXT,
    track_name      TEXT,
    artist_names    JSONB,
    artist_ids      JSONB,
    album_id        TEXT,
    album_name      TEXT,
    duration_ms     INTEGER,
    progress_ms     INTEGER,
    is_playing      BOOLEAN,
    source_key      TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.loaded_files (
    source_key TEXT PRIMARY KEY,
    loaded_at  TIMESTAMPTZ DEFAULT now()
);
"""


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        user=os.getenv("POSTGRES_USER", "spotify"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB", "spotify_dw"),
    )


def list_keys():
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="raw/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                yield obj["Key"]


def event_id(e):
    return hashlib.md5(f"{e['track_id']}|{e['timestamp']}".encode()).hexdigest()


def load_once():
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("SELECT source_key FROM raw.loaded_files")
            done = {r[0] for r in cur.fetchall()}

        new_keys = [k for k in list_keys() if k not in done]
        if not new_keys:
            log.info("No new files.")
            return

        for key in sorted(new_keys):
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
            events = [json.loads(line) for line in body.splitlines() if line.strip()]
            rows = [
                (
                    event_id(e), e["timestamp"], e.get("track_id"), e.get("track_name"),
                    Json(e.get("artist_names", [])), Json(e.get("artist_ids", [])),
                    e.get("album_id"), e.get("album_name"), e.get("duration_ms"),
                    e.get("progress_ms"), e.get("is_playing"), key,
                )
                for e in events
            ]
            with conn, conn.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO raw.spotify_events
                       (event_id, event_timestamp, track_id, track_name, artist_names,
                        artist_ids, album_id, album_name, duration_ms, progress_ms,
                        is_playing, source_key)
                       VALUES %s ON CONFLICT (event_id) DO NOTHING""",
                    rows,
                )
                cur.execute(
                    "INSERT INTO raw.loaded_files (source_key) VALUES (%s) ON CONFLICT DO NOTHING",
                    (key,),
                )
            log.info("Loaded %d events from %s", len(rows), key)
    finally:
        conn.close()


if __name__ == "__main__":
    if "--watch" in sys.argv:
        while True:
            try:
                load_once()
            except Exception as exc:
                log.warning("Load failed: %s", exc)
            time.sleep(INTERVAL)
    else:
        load_once()
