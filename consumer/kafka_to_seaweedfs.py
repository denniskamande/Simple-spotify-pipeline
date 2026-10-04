"""Consume Spotify events from Kafka and write batched JSON-lines files to SeaweedFS (S3)."""
import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
BUCKET = os.getenv("S3_BUCKET", "spotify-data")
BATCH_SIZE = 20          # flush after this many events...
BATCH_SECONDS = 60       # ...or this many seconds, whichever comes first

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

running = True
signal.signal(signal.SIGINT, lambda *_: globals().__setitem__("running", False))
signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY", "any"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY", "any"),
    region_name="us-east-1",
)


def ensure_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        log.info("Created bucket %s", BUCKET)


def flush(batch):
    now = datetime.now(timezone.utc)
    key = (
        f"raw/year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"events_{now:%H%M%S}_{uuid.uuid4().hex[:8]}.jsonl"
    )
    body = "\n".join(json.dumps(e) for e in batch).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    log.info("Wrote %d events to s3://%s/%s", len(batch), BUCKET, key)


def main():
    ensure_bucket()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="spotify-s3-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit only after a successful upload
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    batch, last_flush = [], time.time()
    log.info("Consumer started. Topic=%s", TOPIC)

    while running:
        records = consumer.poll(timeout_ms=1000)
        for msgs in records.values():
            batch.extend(m.value for m in msgs)

        due = len(batch) >= BATCH_SIZE or (batch and time.time() - last_flush >= BATCH_SECONDS)
        if due:
            try:
                flush(batch)
                consumer.commit()
                batch, last_flush = [], time.time()
            except Exception as exc:
                log.warning("Flush failed, will retry: %s", exc)
                time.sleep(5)

    if batch:
        flush(batch)
        consumer.commit()
    consumer.close()
    log.info("Consumer stopped.")


if __name__ == "__main__":
    main()
