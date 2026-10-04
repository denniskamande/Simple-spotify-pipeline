"""Poll Spotify for the currently playing track and publish new plays to Kafka."""
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import spotipy
from dotenv import load_dotenv
from kafka import KafkaProducer
from spotipy.oauth2 import SpotifyOAuth

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
POLL_SECONDS = 10
# If progress jumps backwards by more than this, treat it as a replay of the same track.
REPLAY_TOLERANCE_MS = 3000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="user-read-currently-playing",
        cache_path=str(ROOT / ".cache"),
    )
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)


def build_event(current: dict) -> dict:
    track = current["item"]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "track_id": track["id"],
        "track_name": track["name"],
        "artist_names": [a["name"] for a in track["artists"]],
        "artist_ids": [a["id"] for a in track["artists"]],
        "album_id": track["album"]["id"],
        "album_name": track["album"]["name"],
        "duration_ms": track["duration_ms"],
        "progress_ms": current.get("progress_ms"),
        "is_playing": current["is_playing"],
    }


def main():
    last_track_id = None
    last_progress = 0
    backoff = POLL_SECONDS

    log.info("Producer started. Topic=%s", TOPIC)
    while running:
        try:
            current = sp.current_user_playing_track()
            backoff = POLL_SECONDS

            # Skip ads / podcasts / nothing playing / paused
            if (
                current
                and current.get("item")
                and current.get("currently_playing_type") == "track"
                and current.get("is_playing")
            ):
                event = build_event(current)
                progress = event["progress_ms"] or 0
                is_new = event["track_id"] != last_track_id
                is_replay = (
                    event["track_id"] == last_track_id
                    and progress < last_progress - REPLAY_TOLERANCE_MS
                )

                if is_new or is_replay:
                    producer.send(TOPIC, value=event)
                    producer.flush()
                    log.info(
                        "NEW PLAY: %s - %s",
                        ", ".join(event["artist_names"]),
                        event["track_name"],
                    )
                    last_track_id = event["track_id"]

                last_progress = progress
            else:
                log.info("Nothing playing.")

        except Exception as exc:  # network, token refresh, Kafka, rate limit...
            backoff = min(backoff * 2, 120)
            log.warning("Error: %s. Retrying in %ss", exc, backoff)

        for _ in range(int(backoff)):
            if not running:
                break
            time.sleep(1)

    producer.flush()
    producer.close()
    log.info("Producer stopped.")


if __name__ == "__main__":
    main()
