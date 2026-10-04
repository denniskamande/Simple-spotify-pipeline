"""Fetch genres for every artist in raw.spotify_events from MusicBrainz -> raw.artist_genres.

(Spotify no longer returns artist genres to this app, so MusicBrainz is used instead.)

    python loader/artist_genres.py
    python loader/artist_genres.py --refresh      # also re-fetch rows older than 90 days
"""
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

API = "https://musicbrainz.org/ws/2/artist"
CONTACT = os.getenv("MUSICBRAINZ_CONTACT", "personal-project")
HEADERS = {"User-Agent": f"spotify-realtime-pipeline/1.0 ( {CONTACT} )"}
PAUSE_SECONDS = 1.1      # MusicBrainz allows about 1 request per second
REFRESH_DAYS = 90
TOP_N = 6                # keep the most-voted genres per artist

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("genres")

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.artist_genres (
    artist_id   TEXT PRIMARY KEY,
    artist_name TEXT,
    genres      JSONB NOT NULL DEFAULT '[]'::jsonb,
    status      TEXT,
    fetched_at  TIMESTAMPTZ DEFAULT now()
);
"""

PENDING = """
SELECT DISTINCT ON (i.artist_id) i.artist_id, n.artist_name
FROM raw.spotify_events e,
     jsonb_array_elements_text(e.artist_ids)   WITH ORDINALITY AS i(artist_id, pos),
     jsonb_array_elements_text(e.artist_names) WITH ORDINALITY AS n(artist_name, pos2)
WHERE i.pos = n.pos2
  AND i.artist_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM raw.artist_genres g
      WHERE g.artist_id = i.artist_id
        AND g.status IN ('ok', 'not_found')
        AND (%(days)s = 0 OR g.fetched_at > now() - make_interval(days => %(days)s))
  )
"""

UPSERT = """
INSERT INTO raw.artist_genres (artist_id, artist_name, genres, status, fetched_at)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (artist_id) DO UPDATE
SET artist_name = EXCLUDED.artist_name, genres = EXCLUDED.genres,
    status = EXCLUDED.status, fetched_at = now()
"""


def get(url, params):
    for attempt in range(3):
        resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
        if resp.status_code in (429, 503):
            time.sleep(5 * (attempt + 1))
            continue
        resp.raise_for_status()
        time.sleep(PAUSE_SECONDS)
        return resp.json()
    raise RuntimeError("MusicBrainz rate limit, try again later")


def lookup_genres(name):
    safe = name.replace('"', '\\"')
    found = get(API, {"query": f'artist:"{safe}"', "fmt": "json", "limit": 5}).get("artists", [])

    exact = [a for a in found if a.get("name", "").lower() == name.lower()]
    if exact:
        match = exact[0]
    elif found and found[0].get("score", 0) >= 95:
        match = found[0]
    else:
        return []

    detail = get(f"{API}/{match['id']}", {"inc": "genres+tags", "fmt": "json"})
    items = detail.get("genres") or detail.get("tags") or []
    items = sorted(items, key=lambda x: x.get("count", 0), reverse=True)
    return [i["name"].lower() for i in items[:TOP_N]]


def main():
    refresh = "--refresh" in sys.argv
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        user=os.getenv("POSTGRES_USER", "spotify"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB", "spotify_dw"),
    )

    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(PENDING, {"days": REFRESH_DAYS if refresh else 0})
        artists = cur.fetchall()

    log.info("%d artist(s) to look up (about %d seconds).", len(artists), int(len(artists) * 2.3))
    found = missing = failed = 0

    for artist_id, name in artists:
        try:
            genres = lookup_genres(name)
        except Exception as exc:
            failed += 1
            log.warning("Lookup failed for %s: %s", name, exc)
            continue

        status = "ok" if genres else "not_found"
        found += 1 if genres else 0
        missing += 0 if genres else 1
        with conn, conn.cursor() as cur:
            cur.execute(UPSERT, (artist_id, name, Json(genres), status))
        log.info("%s -> %s", name, ", ".join(genres) if genres else "(not found)")

    conn.close()
    log.info("Done. With genres: %d | not found: %d | failed: %d", found, missing, failed)


if __name__ == "__main__":
    main()
