"""Independent validation for a completed yearly NYC 311 raw download."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import pathlib
import sqlite3
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    args = parser.parse_args()
    raw_root = ROOT / "data" / "raw" / "nyc311" / f"year={args.year}"
    report_dir = ROOT / "reports" / "ingest"
    report_dir.mkdir(parents=True, exist_ok=True)
    db_path = report_dir / f"validation_keys_{args.year}.sqlite"
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("CREATE TABLE keys (unique_key TEXT PRIMARY KEY) WITHOUT ROWID")
    started = time.monotonic()
    total_rows = 0
    duplicate_count = 0
    duplicate_examples = []
    months = []
    try:
        for month in range(1, 13):
            month_dir = raw_root / f"month={month:02d}"
            manifest = json.loads((month_dir / "manifest.json").read_text(encoding="utf-8"))
            if not manifest.get("complete"):
                raise RuntimeError(f"month not marked complete: {month:02d}")
            month_rows = 0
            previous_key = None
            for part in manifest["parts"]:
                path = month_dir / part["file"]
                if file_sha256(path) != part["compressed_sha256"]:
                    raise RuntimeError(f"compressed hash mismatch: {path}")
                uncompressed = hashlib.sha256()
                part_rows = 0
                key_batch = []
                with gzip.open(path, "rb") as stream:
                    for raw_line in stream:
                        uncompressed.update(raw_line)
                        row = json.loads(raw_line)
                        order_key = (row["created_date"], row["unique_key"])
                        if previous_key is not None and order_key <= previous_key:
                            raise RuntimeError(f"order violation: {path} {previous_key} -> {order_key}")
                        previous_key = order_key
                        key_batch.append((row["unique_key"],))
                        part_rows += 1
                if part_rows != part["rows"]:
                    raise RuntimeError(f"part row mismatch: {path}")
                if uncompressed.hexdigest() != part["uncompressed_sha256"]:
                    raise RuntimeError(f"uncompressed hash mismatch: {path}")
                before = connection.total_changes
                connection.executemany("INSERT OR IGNORE INTO keys(unique_key) VALUES (?)", key_batch)
                inserted = connection.total_changes - before
                duplicates = part_rows - inserted
                if duplicates and len(duplicate_examples) < 10:
                    for (key,) in key_batch:
                        occurrences = connection.execute("SELECT COUNT(*) FROM keys WHERE unique_key=?", (key,)).fetchone()[0]
                        if occurrences and key in duplicate_examples:
                            continue
                    duplicate_examples.append(f"{duplicates} duplicate(s) in {month:02d}/{part['file']}")
                duplicate_count += duplicates
                month_rows += part_rows
            connection.commit()
            if month_rows != manifest["rows"] or month_rows != manifest["source_count_start"] or month_rows != manifest["source_count_final"]:
                raise RuntimeError(f"month reconciliation mismatch: {month:02d}")
            total_rows += month_rows
            months.append({"month": month, "rows": month_rows, "parts": len(manifest["parts"]), "first_key": manifest["parts"][0]["first_key"], "last_key": manifest["parts"][-1]["last_key"]})
            print(f"validated {args.year}-{month:02d}: {month_rows} rows", flush=True)
        unique_keys = connection.execute("SELECT COUNT(*) FROM keys").fetchone()[0]
    finally:
        connection.close()
    report = {"year": args.year, "rows": total_rows, "unique_keys": unique_keys, "duplicate_count": duplicate_count, "duplicate_examples": duplicate_examples, "months": months, "seconds": round(time.monotonic() - started, 3), "checks": {"all_12_months": len(months) == 12, "keys_unique": total_rows == unique_keys and duplicate_count == 0, "monthly_counts_reconciled": True, "file_hashes_verified": True, "strict_order_verified": True}}
    output = report_dir / f"validation_{args.year}.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not all(report["checks"].values()):
        raise SystemExit(json.dumps(report["checks"]))
    print(json.dumps({"report": str(output), "rows": total_rows, "unique_keys": unique_keys, "seconds": report["seconds"], "checks": report["checks"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
