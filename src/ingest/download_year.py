"""Resumable NYC 311 yearly downloader using monthly keyset pagination."""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
DATASET_ID = "erm2-nwe9"
BASE = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
FIELDS = [":updated_at", "unique_key", "created_date", "closed_date", "agency", "agency_name", "complaint_type", "descriptor", "location_type", "incident_zip", "status", "due_date", "resolution_action_updated_date", "community_board", "council_district", "police_precinct", "borough", "open_data_channel_type", "latitude", "longitude"]


def atomic_json(path: pathlib.Path, payload: dict) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def request_json(params: dict[str, str], attempts: int = 6):
    url = BASE + "?" + urllib.parse.urlencode(params)
    last_error = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "nyc311-warehouse-portfolio/0.1"})
            with urllib.request.urlopen(req, timeout=180) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in (429, 500, 502, 503, 504) or attempt + 1 == attempts:
                break
            retry_after = exc.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 30))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


def month_bounds(year: int, month: int):
    start = dt.datetime(year, month, 1)
    end = dt.datetime(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return start.strftime("%Y-%m-%dT%H:%M:%S.000"), end.strftime("%Y-%m-%dT%H:%M:%S.000")


def source_count(start: str, end: str) -> int:
    rows = request_json({"$select": "count(*) as n", "$where": f"created_date >= '{start}' AND created_date < '{end}'"})
    return int(rows[0]["n"])


def verify_existing_parts(month_dir: pathlib.Path, state: dict) -> None:
    for part in state["parts"]:
        path = month_dir / part["file"]
        if not path.is_file() or path.stat().st_size != part["compressed_bytes"]:
            raise RuntimeError(f"missing or size-changed completed part: {path}")


def download_month(year: int, month: int, page_size: int, output_root: pathlib.Path) -> dict:
    start, end = month_bounds(year, month)
    month_dir = output_root / f"year={year}" / f"month={month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)
    state_path = month_dir / "manifest.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        verify_existing_parts(month_dir, state)
        if state.get("complete"):
            current_count = source_count(start, end)
            if current_count != state["source_count_final"]:
                raise RuntimeError(f"source count changed for completed {year}-{month:02d}: {state['source_count_final']} -> {current_count}")
            print(f"{year}-{month:02d} already complete: {state['rows']} rows", flush=True)
            return state
    else:
        state = {"dataset_id": DATASET_ID, "year": year, "month": month, "scope_start": start, "scope_end": end, "fields": FIELDS, "page_size": page_size, "source_count_start": source_count(start, end), "rows": 0, "parts": [], "last_created_date": None, "last_unique_key": None, "complete": False, "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
        atomic_json(state_path, state)

    began = time.monotonic()
    while state["rows"] < state["source_count_start"]:
        where = f"created_date >= '{start}' AND created_date < '{end}'"
        if state["last_created_date"] is not None:
            last_date = state["last_created_date"].replace("'", "''")
            last_key = state["last_unique_key"].replace("'", "''")
            where += f" AND (created_date > '{last_date}' OR (created_date = '{last_date}' AND unique_key > '{last_key}'))"
        rows = request_json({"$select": ",".join(FIELDS), "$where": where, "$order": "created_date ASC, unique_key ASC", "$limit": str(page_size)})
        if not rows:
            break
        part_number = len(state["parts"])
        filename = f"part-{part_number:05d}.jsonl.gz"
        final_path = month_dir / filename
        temp_path = month_dir / (filename + ".tmp")
        uncompressed_hash = hashlib.sha256()
        with gzip.open(temp_path, "wb", compresslevel=6) as output:
            for row in rows:
                line = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
                output.write(line)
                uncompressed_hash.update(line)
        temp_path.replace(final_path)
        compressed_hash = hashlib.sha256(final_path.read_bytes()).hexdigest()
        first_key = [rows[0]["created_date"], rows[0]["unique_key"]]
        last_key = [rows[-1]["created_date"], rows[-1]["unique_key"]]
        if state["last_created_date"] is not None and tuple(first_key) <= (state["last_created_date"], state["last_unique_key"]):
            raise RuntimeError(f"non-increasing page boundary at {year}-{month:02d} {filename}")
        state["parts"].append({"file": filename, "rows": len(rows), "first_key": first_key, "last_key": last_key, "compressed_bytes": final_path.stat().st_size, "compressed_sha256": compressed_hash, "uncompressed_sha256": uncompressed_hash.hexdigest()})
        state["rows"] += len(rows)
        state["last_created_date"], state["last_unique_key"] = last_key
        atomic_json(state_path, state)
        print(f"{year}-{month:02d} part {part_number:03d}: +{len(rows)} rows, total {state['rows']}/{state['source_count_start']}", flush=True)
        if len(rows) < page_size:
            break

    state["source_count_final"] = source_count(start, end)
    state["complete"] = state["rows"] == state["source_count_start"] == state["source_count_final"]
    state["finished_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["run_seconds_latest"] = round(time.monotonic() - began, 3)
    atomic_json(state_path, state)
    if not state["complete"]:
        raise RuntimeError(f"month reconciliation failed for {year}-{month:02d}: downloaded={state['rows']} start={state['source_count_start']} final={state['source_count_final']}")
    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--page-size", type=int, default=20000)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    args = parser.parse_args()
    if not 1 <= args.start_month <= args.end_month <= 12:
        raise SystemExit("months must satisfy 1 <= start <= end <= 12")
    if not 1000 <= args.page_size <= 50000:
        raise SystemExit("page-size must be between 1000 and 50000")
    output_root = ROOT / "data" / "raw" / "nyc311"
    states = [download_month(args.year, month, args.page_size, output_root) for month in range(args.start_month, args.end_month + 1)]
    report = {"dataset_id": DATASET_ID, "year": args.year, "months": [s["month"] for s in states], "rows": sum(s["rows"] for s in states), "all_complete": all(s["complete"] for s in states), "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    report_dir = ROOT / "reports" / "ingest"
    report_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(report_dir / f"download_{args.year}.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
