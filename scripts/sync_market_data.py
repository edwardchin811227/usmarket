#!/usr/bin/env python3
import argparse
import csv
import re
from datetime import datetime
from pathlib import Path

REQUIRED = [
    "Date",
    "BTC_USD",
    "USD_Index",
    "TLT",
    "HYG",
    "US10Y_Yield",
    "VIX",
    "Nasdaq100",
    "SP500",
]

NUMERIC_COLS = [
    "BTC_USD",
    "USD_Index",
    "TLT",
    "HYG",
    "US10Y_Yield",
    "VIX",
    "Nasdaq100",
    "SP500",
]


def norm_date(raw: str) -> str:
    s = (raw or "").strip()
    m = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", s)
    if not m:
        raise ValueError(f"Invalid date: {raw}")
    y, mm, dd = m.groups()
    dt = datetime(int(y), int(mm), int(dd))
    return dt.strftime("%Y-%m-%d")


def fmt_number(col: str, raw: str) -> str:
    s = (raw or "").strip()
    if s == "":
        return ""
    v = float(s)
    if col == "BTC_USD":
        return str(int(round(v)))
    return f"{v:.2f}"


def read_rows(src: Path) -> list[dict]:
    with src.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("Source CSV has no header")
        miss = [c for c in REQUIRED if c not in reader.fieldnames]
        if miss:
            raise ValueError(f"Source CSV missing columns: {', '.join(miss)}")

        rows = []
        for row in reader:
            out = {"Date": norm_date(row.get("Date", ""))}
            for c in NUMERIC_COLS:
                out[c] = fmt_number(c, row.get(c, ""))
            rows.append(out)
    rows.sort(key=lambda r: r["Date"])
    return rows


def write_rows(dst: Path, rows: list[dict]) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    p = argparse.ArgumentParser(description="Sync 8-factors CSV into GitHub Pages data file")
    p.add_argument(
        "--source",
        default=r"C:\Users\av_ch\Downloads\8 Factors\8 factors - 8 factors.csv",
        help="Source CSV path",
    )
    p.add_argument(
        "--output",
        default=r"C:\Users\av_ch\Documents\GitHub\usmarket\data\8-factors.csv",
        help="Output CSV path inside repo",
    )
    args = p.parse_args()

    src = Path(args.source)
    dst = Path(args.output)
    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src}")

    rows = read_rows(src)
    write_rows(dst, rows)

    print(f"source={src}")
    print(f"output={dst}")
    print(f"rows={len(rows)}")
    if rows:
        print(f"range={rows[0]['Date']}..{rows[-1]['Date']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
