# seed-alert — Usage Guide

A small producer utility that reads alert rows from a CSV and pushes each one as a
job onto the **same BullMQ queue** the Defect Identification Service consumes from
(`bull:defectIdentificationQueue:wait`). The worker then picks them up exactly as if
they had arrived from the Rule Engine.

---

## 1. Prerequisites

- Python 3.11+
- A reachable Redis instance (the same one the service uses)
- Install dependencies:

  ```bash
  cd seed-alert
  pip install -r requirements.txt
  ```

  Or reuse the service's virtual environment:

  ```bash
  ../defect-identification-service/.venv/bin/python seed.py --dry-run
  ```

---

## 2. Configuration (`.env`)

Values are read from `.env` (via `python-dotenv`) and can be overridden on the CLI.

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string. Keep in sync with the service. |
| `BULLMQ_PREFIX` | `bull` | BullMQ key prefix. |
| `DEFECT_QUEUE_NAME` | `defectIdentificationQueue` | Queue name. Must match the consumer. |
| `CSV_PATH` | `data.csv` | Path to the CSV of alerts. |
| `SEED_INTERVAL` | `0` | Seconds between repeated pushes (`0` = run once). |
| `SEED_COUNT` | `0` | Number of passes when repeating (`0` = forever). |

> ⚠️ The committed `.env` points at a **remote** Redis. Point `REDIS_URL` at a local
> Redis while testing so you don't seed a shared/production queue.

---

## 3. CSV format

The CSV **must** have this exact header. Each column maps to a field the service's
preprocessor reads out of the job payload:

```csv
causeDescription,timestamp,vesselMappingName,vesselId,tenant
Main Engine High Temperature,2026-07-10T09:15:00,MV Orion Star,101,gesco
Fuel Oil Low Pressure,2026-07-10T09:20:00,MV Orion Star,101,gesco
```

| CSV column | Job payload key | Service field | Type |
|---|---|---|---|
| `causeDescription` | `causeDescription` | `alarm_name` | string |
| `timestamp` | `timestamp` | `timestamp` | string (ISO 8601) |
| `vesselMappingName` | `vesselMappingName` | `vessel_mapping_name` | string |
| `vesselId` | `vesselId` | `vessel_id` | integer |
| `tenant` | `tenant` | `tenant` | string |

Missing columns cause the run to abort with an error listing what's missing. Blank
rows are skipped.

---

## 4. Running

### Preview without touching Redis (dry-run)

```bash
python3 seed.py --dry-run
```

Prints the exact JSON payload for each row, then exits.

### Push once

```bash
python3 seed.py
```

### Use a different CSV

```bash
python3 seed.py --csv other-alerts.csv
```

### Repeat on an interval (in-process loop)

```bash
python3 seed.py --interval 15            # re-push the whole CSV every 15s, forever
python3 seed.py --interval 60 --count 10 # every 60s, 10 passes, then stop
```

Best for **sub-minute** frequency. Stop with `Ctrl-C`.

---

## 5. Scheduling with cron

For **minute-or-longer** frequency that survives reboots, use cron with the wrapper
script (`run-seed.sh`), which cd's into this folder, prefers the service venv, runs
once, and appends output to `seed-alert.log`.

```bash
crontab -e     # paste a line from crontab.example, e.g. every 5 minutes:
*/5 * * * * /home/developer/Desktop/DAY\ TO\ DAY/defect-management/seed-alert/run-seed.sh
```

See `crontab.example` for ready-made schedule lines. Verify with `crontab -l`.

> Cron's finest granularity is **1 minute**. For faster cadence, use `--interval`.

---

## 6. CLI reference

| Flag | Env fallback | Default | Description |
|---|---|---|---|
| `--csv` | `CSV_PATH` | `data.csv` | CSV file to read. |
| `--redis-url` | `REDIS_URL` | `redis://localhost:6379` | Redis connection URL. |
| `--prefix` | `BULLMQ_PREFIX` | `bull` | BullMQ key prefix. |
| `--queue` | `DEFECT_QUEUE_NAME` | `defectIdentificationQueue` | Queue name. |
| `--interval` | `SEED_INTERVAL` | `0` | Seconds between passes (`0` = once). |
| `--count` | `SEED_COUNT` | `0` | Passes when repeating (`0` = forever). |
| `--dry-run` | — | off | Print payloads, push nothing. |

---

## 7. How it works

For every CSV row, one BullMQ job is created using the standard protocol:

1. `INCR bull:{queue}:id` → next monotonic job id
2. `HSET bull:{queue}:{id}` → job hash (`name`, `data` = JSON payload, `opts`, `timestamp`…)
3. `LPUSH bull:{queue}:wait {id}` → enqueue

The consumer (`defect-identification-service/src/inbound/queue_listener.py`) then
does `BLMOVE wait → active` (RIGHT→LEFT, FIFO), reads the `data` field, and runs it
through preprocessing → core → outbound.

---

## 8. Notes & gotchas

- **Each pass pushes new jobs** with fresh IDs, so the queue grows by `len(csv)` per
  cycle — make sure the worker is running to drain it.
- **Meaningful defects require matching mappings.** The core processor only records a
  defect when `causeDescription` + `vesselId` match an existing `AlarmDefectMapping`
  row in Postgres; otherwise the job is ACKed with "no mapping found". Populate the
  CSV with alarm names / vessel IDs that exist in the mapping table for end-to-end
  results.
- **Connection errors** abort immediately with a clear message (the tool `PING`s
  Redis before pushing).
