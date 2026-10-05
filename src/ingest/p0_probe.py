"""P0: bounded NYC311 counts, download benchmark, and capacity evidence."""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import gzip
import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "p0.json"
API = "https://data.cityofnewyork.us/resource/{dataset_id}.json"
FIELDS = [":updated_at", "unique_key", "created_date", "closed_date", "agency", "agency_name", "complaint_type", "descriptor", "location_type", "incident_zip", "status", "due_date", "resolution_action_updated_date", "community_board", "council_district", "police_precinct", "borough", "open_data_channel_type", "latitude", "longitude"]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def request_json(url: str, attempts: int = 5):
    last_error = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "nyc311-warehouse-portfolio/0.1"})
            with urllib.request.urlopen(req, timeout=120) as response:
                return json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(2 ** attempt, 15))
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


def api_url(base: str, params: dict[str, str]) -> str:
    return base + "?" + urllib.parse.urlencode(params)


def month_ranges(year: int):
    for month in range(1, 13):
        start = dt.datetime(year, month, 1)
        end = dt.datetime(year + (month == 12), 1 if month == 12 else month + 1, 1)
        yield start, end


def count_month(base: str, pair):
    start, end = pair
    where = f"created_date >= '{start:%Y-%m-%dT%H:%M:%S.000}' AND created_date < '{end:%Y-%m-%dT%H:%M:%S.000}'"
    url = api_url(base, {"$select": "count(*) as n", "$where": where})
    began = time.monotonic()
    rows = request_json(url)
    return {"month": start.strftime("%Y-%m"), "count": int(rows[0]["n"]), "seconds": round(time.monotonic() - began, 3), "url": url}


def command_version(command: list[str]):
    try:
        proc = subprocess.run(command, text=True, capture_output=True, timeout=15)
        output = (proc.stdout or proc.stderr).strip().splitlines()
        return {"available": proc.returncode == 0, "first_line": output[0] if output else "", "returncode": proc.returncode}
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "error": type(exc).__name__}


def environment_report():
    usage = shutil.disk_usage(ROOT.anchor)
    return {"checked_at_utc": utc_now(), "platform": platform.platform(), "python": platform.python_version(), "cpu_threads": os.cpu_count(), "project_drive_total_gib": round(usage.total / 2**30, 2), "project_drive_free_gib": round(usage.free / 2**30, 2), "java": command_version(["java", "-version"]), "mysql": command_version(["mysql", "--version"]), "docker": command_version(["docker", "--version"]), "spark_submit": command_version(["spark-submit", "--version"])}


def download_sample(base: str, cfg: dict, limit: int, raw_dir: pathlib.Path):
    raw_dir.mkdir(parents=True, exist_ok=True)
    final_path = raw_dir / f"nyc311_sample_{limit}.jsonl.gz"
    temp_path = final_path.with_suffix(final_path.suffix + ".part")
    if temp_path.exists():
        temp_path.unlink()
    page_size = min(int(cfg["page_size"]), limit)
    total = 0
    pages = []
    last_date = last_key = None
    started = time.monotonic()
    uncompressed_bytes = 0
    digest = hashlib.sha256()
    base_where = f"created_date >= '{cfg['sample_start']}' AND created_date < '{cfg['sample_end']}'"
    with gzip.open(temp_path, "wb", compresslevel=6) as output:
        while total < limit:
            current_limit = min(page_size, limit - total)
            where = base_where
            if last_date is not None:
                safe_date = last_date.replace("'", "''")
                safe_key = last_key.replace("'", "''")
                where += f" AND (created_date > '{safe_date}' OR (created_date = '{safe_date}' AND unique_key > '{safe_key}'))"
            params = {"$select": ",".join(FIELDS), "$where": where, "$order": "created_date ASC, unique_key ASC", "$limit": str(current_limit)}
            rows = request_json(api_url(base, params))
            if not rows:
                break
            page_started = time.monotonic()
            for row in rows:
                encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
                output.write(encoded)
                digest.update(encoded)
                uncompressed_bytes += len(encoded)
            total += len(rows)
            last_date, last_key = rows[-1]["created_date"], rows[-1]["unique_key"]
            pages.append({"rows": len(rows), "write_seconds": round(time.monotonic() - page_started, 3), "last_created_date": last_date, "last_unique_key": last_key})
            if len(rows) < current_limit:
                break
    temp_path.replace(final_path)
    elapsed = time.monotonic() - started
    return {"path": str(final_path.relative_to(ROOT)), "rows": total, "pages": pages, "seconds": round(elapsed, 3), "rows_per_second": round(total / elapsed, 2) if elapsed else None, "compressed_bytes": final_path.stat().st_size, "uncompressed_bytes": uncompressed_bytes, "uncompressed_sha256": digest.hexdigest(), "sample_scope": [cfg["sample_start"], cfg["sample_end"]], "complete_requested_limit": total == limit}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--counts-only", action="store_true")
    parser.add_argument("--sample-limit", type=int)
    args = parser.parse_args()
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    base = API.format(dataset_id=cfg["dataset_id"])
    report_dir = ROOT / "reports" / "p0"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = {"report_version": 1, "started_at_utc": utc_now(), "config": cfg, "environment": environment_report()}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        report["monthly_counts"] = sorted(pool.map(lambda pair: count_month(base, pair), month_ranges(2025)), key=lambda x: x["month"])
    report["baseline_count"] = sum(item["count"] for item in report["monthly_counts"])
    if not args.counts_only:
        limit = args.sample_limit or int(cfg["sample_limit"])
        if limit <= 0 or limit > 100000:
            raise SystemExit("P0 sample-limit must be between 1 and 100000")
        report["sample"] = download_sample(base, cfg, limit, ROOT / "data" / "raw" / "p0")
        per_row = report["sample"]["compressed_bytes"] / report["sample"]["rows"]
        report["rough_raw_compressed_projection"] = {"bytes_per_sample_row": round(per_row, 2), "baseline_gib": round(per_row * report["baseline_count"] / 2**30, 3), "warning": "Raw gzip projection from one January sample only; database, indexes, Parquet, temp and logs are not included."}
    report["finished_at_utc"] = utc_now()
    suffix = "counts" if args.counts_only else f"sample_{report['sample']['rows']}"
    path = report_dir / f"p0_{suffix}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(path), "baseline_count": report["baseline_count"], "sample": report.get("sample"), "environment": report["environment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
