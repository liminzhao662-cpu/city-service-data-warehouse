"""Validate the frozen P0 gzip JSONL sample independently of the downloader."""
from __future__ import annotations

import collections
import gzip
import hashlib
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data" / "raw" / "p0" / "nyc311_sample_50000.jsonl.gz"
DOWNLOAD_REPORT = ROOT / "reports" / "p0" / "p0_sample_50000.json"
OUTPUT = ROOT / "reports" / "p0" / "p0_sample_validation.json"
REQUIRED = {"unique_key", "created_date", "agency", "complaint_type", "status", "borough"}


def main():
    download = json.loads(DOWNLOAD_REPORT.read_text(encoding="utf-8"))
    count = 0
    keys = set()
    duplicate_examples = []
    previous = None
    order_violations = []
    missing = collections.Counter()
    statuses = collections.Counter()
    agencies = collections.Counter()
    fields = set()
    digest = hashlib.sha256()
    first_key = last_key = None
    with gzip.open(SOURCE, "rb") as stream:
        for raw_line in stream:
            digest.update(raw_line)
            row = json.loads(raw_line)
            count += 1
            fields.update(row)
            key = row.get("unique_key")
            order_key = (row.get("created_date", ""), key or "")
            if first_key is None:
                first_key = order_key
            last_key = order_key
            if previous is not None and order_key <= previous and len(order_violations) < 10:
                order_violations.append({"previous": previous, "current": order_key})
            previous = order_key
            if key in keys and len(duplicate_examples) < 10:
                duplicate_examples.append(key)
            keys.add(key)
            for field in REQUIRED:
                if row.get(field) in (None, ""):
                    missing[field] += 1
            statuses[row.get("status", "<MISSING>")] += 1
            agencies[row.get("agency", "<MISSING>")] += 1
    expected = download["sample"]
    report = {
        "source": str(SOURCE.relative_to(ROOT)),
        "rows": count,
        "unique_keys": len(keys),
        "duplicate_count": count - len(keys),
        "duplicate_examples": duplicate_examples,
        "strict_order_violations": order_violations,
        "first_order_key": first_key,
        "last_order_key": last_key,
        "required_missing_counts": dict(sorted(missing.items())),
        "status_counts": dict(statuses.most_common()),
        "top_agencies": dict(agencies.most_common(15)),
        "observed_fields": sorted(fields),
        "uncompressed_sha256": digest.hexdigest(),
        "checks": {
            "row_count_matches_download": count == expected["rows"],
            "hash_matches_download": digest.hexdigest() == expected["uncompressed_sha256"],
            "keys_unique": count == len(keys),
            "strictly_sorted": not order_violations,
            "required_fields_observed": REQUIRED.issubset(fields),
        },
        "scope_note": "First 50,000 rows of January 2025 ordered by (created_date, unique_key); it is not a random sample or the complete month."
    }
    OUTPUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not all(report["checks"].values()):
        raise SystemExit(json.dumps(report["checks"], ensure_ascii=False))
    print(json.dumps({"output": str(OUTPUT), "checks": report["checks"], "first": first_key, "last": last_key}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
