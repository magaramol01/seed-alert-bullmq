"""
seed-alert — CSV → BullMQ producer for the Defect Identification Service.

Reads alert rows from a CSV and pushes each one as a job onto the same BullMQ
queue the service consumes from (`bull:defectIdentificationQueue:wait`), so the
worker (src/inbound/queue_listener.py) picks them up exactly as if they came
from the Rule Engine.

BullMQ job protocol replicated here (matching what the consumer expects):
  1. INCR  bull:{queue}:id            → next job id (monotonic)
  2. HSET  bull:{queue}:{id}          → job hash (name, data, opts, timestamp …)
  3. LPUSH bull:{queue}:wait  {id}    → enqueue (consumer BLMOVEs RIGHT→LEFT = FIFO)

The consumer reads the JSON `data` field and maps these keys
(see src/preprocessing/preprocessor.py):
    causeDescription → alarm_name
    timestamp        → timestamp
    vesselMappingName→ vessel_mapping_name
    vesselId         → vessel_id (int)
    tenant           → tenant

CSV columns must therefore be:
    causeDescription, timestamp, vesselMappingName, vesselId, tenant

Usage:
    python seed.py                       # uses .env / data.csv
    python seed.py --csv other.csv       # different CSV
    python seed.py --dry-run             # print jobs without pushing
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import redis
from dotenv import load_dotenv

# Fields the consumer's preprocessor reads out of the job `data` payload.
REQUIRED_COLUMNS = [
    "causeDescription",
    "timestamp",
    "vesselMappingName",
    "vesselId",
    "tenant",
]


def build_payload(row: dict[str, str]) -> dict:
    """Map one CSV row to the raw JSON payload the service expects."""
    return {
        "causeDescription": (row.get("causeDescription") or "").strip(),
        "timestamp": (row.get("timestamp") or "").strip(),
        "vesselMappingName": (row.get("vesselMappingName") or "").strip(),
        "vesselId": int(row["vesselId"]) if str(row.get("vesselId") or "").strip() else 0,
        "tenant": (row.get("tenant") or "").strip(),
    }


def enqueue_job(client: redis.Redis, prefix: str, queue: str, payload: dict) -> str:
    """Create a BullMQ job for `payload` and push it onto the wait list."""
    job_id = str(client.incr(f"{prefix}:{queue}:id"))
    job_key = f"{prefix}:{queue}:{job_id}"
    now_ms = int(time.time() * 1000)

    job_hash = {
        "name": "seed-alert",
        "data": json.dumps(payload, separators=(",", ":")),
        "opts": json.dumps({"attempts": 1}, separators=(",", ":")),
        "timestamp": now_ms,
        "delay": 0,
        "priority": 0,
    }

    pipe = client.pipeline(transaction=True)
    pipe.hset(job_key, mapping=job_hash)
    pipe.lpush(f"{prefix}:{queue}:wait", job_id)
    pipe.execute()
    return job_id


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        sys.exit(f"[seed-alert] CSV not found: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        if missing:
            sys.exit(
                f"[seed-alert] CSV is missing required column(s): {', '.join(missing)}\n"
                f"             Expected header: {', '.join(REQUIRED_COLUMNS)}"
            )
        return [row for row in reader if any((v or '').strip() for v in row.values())]


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Push CSV alerts onto the defect BullMQ queue.")
    parser.add_argument("--csv", default=os.getenv("CSV_PATH", "data.csv"),
                        help="Path to the CSV file (default: %(default)s).")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379"),
                        help="Redis connection URL.")
    parser.add_argument("--prefix", default=os.getenv("BULLMQ_PREFIX", "bull"),
                        help="BullMQ key prefix (default: %(default)s).")
    parser.add_argument("--queue", default=os.getenv("DEFECT_QUEUE_NAME", "defectIdentificationQueue"),
                        help="Queue name (default: %(default)s).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the jobs that would be pushed without touching Redis.")
    parser.add_argument("--interval", type=float, default=float(os.getenv("SEED_INTERVAL", "0")),
                        help="Re-push the whole CSV every N seconds (0 = run once and exit).")
    parser.add_argument("--count", type=int, default=int(os.getenv("SEED_COUNT", "0")),
                        help="With --interval, stop after this many passes (0 = run forever).")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = (Path(__file__).parent / csv_path).resolve()

    rows = load_rows(csv_path)
    print(f"[seed-alert] loaded {len(rows)} row(s) from {csv_path}")

    if args.dry_run:
        for i, row in enumerate(rows, 1):
            print(f"  [{i}] {json.dumps(build_payload(row))}")
        print("[seed-alert] dry-run complete — nothing pushed.")
        return

    client = redis.from_url(args.redis_url, decode_responses=True)
    try:
        client.ping()
    except redis.exceptions.RedisError as exc:
        sys.exit(f"[seed-alert] cannot connect to Redis at {args.redis_url}: {exc}")

    wait_key = f"{args.prefix}:{args.queue}:wait"
    print(f"[seed-alert] pushing to queue '{args.queue}' (wait key: {wait_key})")

    if args.interval <= 0:
        push_once(client, args, rows)
        return

    # Repeating mode — re-push the CSV every `interval` seconds.
    mode = "forever" if args.count <= 0 else f"{args.count} pass(es)"
    print(f"[seed-alert] repeating every {args.interval}s ({mode}) — Ctrl-C to stop.")
    passes = 0
    try:
        while True:
            passes += 1
            print(f"[seed-alert] --- pass {passes} ---")
            push_once(client, args, rows)
            if args.count and passes >= args.count:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n[seed-alert] stopped after {passes} pass(es).")


def push_once(client: redis.Redis, args, rows: list[dict[str, str]]) -> int:
    """Push every row once; returns the number of jobs enqueued."""
    pushed = 0
    for i, row in enumerate(rows, 1):
        try:
            payload = build_payload(row)
        except (ValueError, KeyError) as exc:
            print(f"  [{i}] SKIPPED — bad row {row}: {exc}")
            continue
        job_id = enqueue_job(client, args.prefix, args.queue, payload)
        pushed += 1
        print(f"  [{i}] job {job_id} → {payload['causeDescription']!r} "
              f"(vessel {payload['vesselId']}, tenant {payload['tenant']})")
    print(f"[seed-alert] done — pushed {pushed}/{len(rows)} job(s) onto '{args.queue}'.")
    return pushed


if __name__ == "__main__":
    main()
