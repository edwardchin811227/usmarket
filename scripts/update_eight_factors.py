#!/usr/bin/env python3
import argparse
import csv
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
CSV_COLS = ["Date", "BTC_USD", "USD_Index", "TLT", "HYG", "US10Y_Yield", "VIX", "Nasdaq100", "SP500"]
YF_MAP = {
    "BTC_USD": "BTC-USD",
    "USD_Index": "DX-Y.NYB",
    "TLT": "TLT",
    "HYG": "HYG",
    "VIX": "^VIX",
    "Nasdaq100": "^NDX",
    "SP500": "^GSPC",
}
FRED_URLS = [
    "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10",
    "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10&cosd=2000-01-01",
    "https://fred.stlouisfed.org/series/DGS10/downloaddata/DGS10.csv",
]


def _http_get(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def _parse_date_any(s: str) -> date:
    s = (s or "").strip()
    if not s:
        raise ValueError("empty date")
    parts = s.replace("/", "-").split("-")
    if len(parts) == 3:
        y, m, d = parts
        return date(int(y), int(m), int(d))
    raise ValueError(f"invalid date: {s}")


def _ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _observed_fixed_holiday(y: int, m: int, d: int) -> date:
    dt = date(y, m, d)
    wd = dt.weekday()  # Mon=0..Sun=6
    if wd == 5:
        return dt - timedelta(days=1)
    if wd == 6:
        return dt + timedelta(days=1)
    return dt


def _nth_weekday(y: int, m: int, weekday: int, n: int) -> date:
    d = date(y, m, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    d += timedelta(days=(n - 1) * 7)
    return d


def _last_weekday(y: int, m: int, weekday: int) -> date:
    if m == 12:
        d = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(y, m + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def _easter_sunday(y: int) -> date:
    a = y % 19
    b = y // 100
    c = y % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(y, month, day)


def _us_stock_holidays(y: int) -> set[date]:
    out = set()
    out.add(_observed_fixed_holiday(y, 1, 1))  # New Year
    out.add(_nth_weekday(y, 1, 0, 3))  # MLK Day
    out.add(_nth_weekday(y, 2, 0, 3))  # Presidents Day
    out.add(_easter_sunday(y) - timedelta(days=2))  # Good Friday
    out.add(_last_weekday(y, 5, 0))  # Memorial Day
    out.add(_observed_fixed_holiday(y, 6, 19))  # Juneteenth
    out.add(_observed_fixed_holiday(y, 7, 4))  # Independence Day
    out.add(_nth_weekday(y, 9, 0, 1))  # Labor Day
    out.add(_nth_weekday(y, 11, 3, 4))  # Thanksgiving
    out.add(_observed_fixed_holiday(y, 12, 25))  # Christmas
    return out


def is_us_stock_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in _us_stock_holidays(d.year)


def resolve_market_date(cutoff_hhmm: str = "16:30") -> date:
    now_et = datetime.now(NY_TZ)
    hh, mm = cutoff_hhmm.split(":")
    cutoff = datetime.combine(now_et.date(), time(int(hh), int(mm)), tzinfo=NY_TZ)
    market_day = now_et.date()
    if now_et < cutoff:
        market_day -= timedelta(days=1)
    return market_day


def _yahoo_last_close_with_date_on_or_before(symbol: str, target_date: date) -> Tuple[date, float]:
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        + urllib.parse.quote(symbol, safe="")
        + "?range=20d&interval=1d"
    )
    body = _http_get(url, timeout=20)
    obj = json.loads(body)
    r = (((obj.get("chart") or {}).get("result") or [None])[0]) or {}
    ts = r.get("timestamp") or []
    closes = (((r.get("indicators") or {}).get("quote") or [{}])[0].get("close") or [])
    if not ts or not closes:
        raise RuntimeError(f"Yahoo no data: {symbol}")

    best: Tuple[date, float] | None = None
    for i, t in enumerate(ts):
        if i >= len(closes):
            break
        c = closes[i]
        if c is None:
            continue
        d = datetime.fromtimestamp(int(t), tz=timezone.utc).astimezone(NY_TZ).date()
        if d <= target_date:
            if (best is None) or (d > best[0]):
                best = (d, float(c))
    if best is None:
        raise RuntimeError(f"Yahoo missing close <= {target_date} for {symbol}")
    return best


def _yahoo_last_close_on_or_before(symbol: str, target_date: date) -> float:
    return _yahoo_last_close_with_date_on_or_before(symbol, target_date)[1]


def fetch_yahoo_bundle(target_date: date) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for col, sym in YF_MAP.items():
        out[col] = _yahoo_last_close_on_or_before(sym, target_date)
    return out


def _parse_fred_csv(text: str) -> Dict[str, str]:
    rows = list(csv.reader(text.splitlines()))
    if not rows or len(rows[0]) < 2:
        raise RuntimeError("FRED CSV malformed")
    out = {}
    for r in rows[1:]:
        if len(r) < 2:
            continue
        k = (r[0] or "").strip()
        v = (r[1] or "").strip()
        if k:
            out[k] = v
    return out


@dataclass
class YieldResult:
    value: float
    source_date: str
    source_mode: str


def _parse_fred_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"DGS10 cache not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("DGS10 cache CSV malformed: no header")
        date_col = "observation_date" if "observation_date" in reader.fieldnames else reader.fieldnames[0]
        val_col = "DGS10" if "DGS10" in reader.fieldnames else reader.fieldnames[1]
        out: Dict[str, str] = {}
        for r in reader:
            k = (r.get(date_col) or "").strip()
            v = (r.get(val_col) or "").strip()
            if k:
                out[k] = v
        return out


def fetch_dgs10(
    target_date: date,
    mode: str = "prev",
    cache_csv: str = "",
    fallback: str = "none",
) -> YieldResult:
    dmap: Dict[str, str] | None = None
    source_kind = "FRED_HTTP"
    last_err = None
    for url in FRED_URLS:
        try:
            txt = _http_get(url, timeout=18)
            dmap = _parse_fred_csv(txt)
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    if not dmap and cache_csv:
        try:
            dmap = _parse_fred_file(Path(cache_csv))
            source_kind = "DGS10_CACHE_CSV"
        except Exception as e:  # noqa: BLE001
            last_err = e
    if not dmap:
        if fallback == "yahoo_tnx":
            try:
                tnx_date, tnx_close = _yahoo_last_close_with_date_on_or_before("^TNX", target_date)
                if mode == "exact" and tnx_date != target_date:
                    raise RuntimeError(f"Yahoo TNX exact missing for {_ymd(target_date)}")
                mode_tag = "DGS10_EXACT_YAHOO_TNX" if mode == "exact" else "DGS10_PREV_YAHOO_TNX"
                return YieldResult(value=float(tnx_close), source_date=_ymd(tnx_date), source_mode=mode_tag)
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"FRED DGS10 download failed and cache unavailable: {last_err}") from e
        raise RuntimeError(f"FRED DGS10 download failed and cache unavailable: {last_err}")

    key = _ymd(target_date)
    if mode == "exact":
        v = (dmap.get(key) or "").strip()
        if not v or v == ".":
            raise RuntimeError(f"DGS10 exact missing for {key}")
        return YieldResult(value=float(v), source_date=key, source_mode=f"DGS10_EXACT_{source_kind}")

    # mode == prev: take nearest non-empty <= target day
    valid_days = sorted(k for k, v in dmap.items() if (v or "").strip() not in ("", "."))
    chosen = None
    for k in reversed(valid_days):
        if k <= key:
            chosen = k
            break
    if not chosen:
        raise RuntimeError(f"DGS10 no valid value <= {key}")
    return YieldResult(value=float(dmap[chosen]), source_date=chosen, source_mode=f"DGS10_PREV_{source_kind}")


def read_existing(csv_path: Path) -> List[Dict[str, str]]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            if not any((r.get(c) or "").strip() for c in CSV_COLS):
                continue
            out = {c: (r.get(c) or "").strip() for c in CSV_COLS}
            out["Date"] = _ymd(_parse_date_any(out["Date"]))
            rows.append(out)
    return rows


def _fmt_row(target_date: date, bundle: Dict[str, float], us10y: YieldResult) -> Dict[str, str]:
    return {
        "Date": _ymd(target_date),
        "BTC_USD": str(int(round(bundle["BTC_USD"]))),
        "USD_Index": f"{bundle['USD_Index']:.2f}",
        "TLT": f"{bundle['TLT']:.2f}",
        "HYG": f"{bundle['HYG']:.2f}",
        "US10Y_Yield": f"{us10y.value:.2f}",
        "VIX": f"{bundle['VIX']:.2f}",
        "Nasdaq100": f"{bundle['Nasdaq100']:.2f}",
        "SP500": f"{bundle['SP500']:.2f}",
    }


def write_rows(csv_path: Path, rows: List[Dict[str, str]]) -> None:
    rows.sort(key=lambda r: r["Date"])
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        w.writeheader()
        w.writerows(rows)


def upsert_row(rows: List[Dict[str, str]], new_row: Dict[str, str]) -> Tuple[List[Dict[str, str]], bool]:
    idx = {r["Date"]: i for i, r in enumerate(rows)}
    key = new_row["Date"]
    if key in idx:
        changed = rows[idx[key]] != new_row
        rows[idx[key]] = new_row
        return rows, changed
    rows.append(new_row)
    return rows, True


def main() -> int:
    p = argparse.ArgumentParser(description="Local replacement of Apps Script updateEightFactors")
    p.add_argument(
        "--csv",
        default=r"C:\Users\av_ch\Downloads\8 Factors\8 factors - 8 factors.csv",
        help="Target local CSV path",
    )
    p.add_argument("--cutoff", default="16:30", help="Market cutoff time in ET, format HH:MM")
    p.add_argument("--force-date", default="", help="Force market date (YYYY-MM-DD)")
    p.add_argument(
        "--us10y-mode",
        choices=["prev", "exact"],
        default="prev",
        help="DGS10 mode: prev=latest <= target day, exact=must match target day",
    )
    p.add_argument(
        "--dgs-cache-csv",
        default=r"C:\Users\av_ch\Downloads\8 Factors\DGS10.csv",
        help="Local DGS10 CSV cache fallback path",
    )
    p.add_argument(
        "--us10y-fallback",
        choices=["none", "yahoo_tnx"],
        default="none",
        help="Fallback when FRED and local cache both fail",
    )
    p.add_argument("--skip-non-trading-day", action="store_true", default=False)
    args = p.parse_args()

    target = _parse_date_any(args.force_date) if args.force_date else resolve_market_date(args.cutoff)
    if args.skip_non_trading_day and not is_us_stock_trading_day(target):
        print(f"skip: {target} is not US stock trading day")
        return 0

    bundle = fetch_yahoo_bundle(target)
    us10y = fetch_dgs10(
        target,
        mode=args.us10y_mode,
        cache_csv=args.dgs_cache_csv,
        fallback=args.us10y_fallback,
    )
    new_row = _fmt_row(target, bundle, us10y)

    csv_path = Path(args.csv)
    rows = read_existing(csv_path)
    rows, changed = upsert_row(rows, new_row)
    write_rows(csv_path, rows)

    print(f"csv={csv_path}")
    print(f"target_date={_ymd(target)}")
    print(f"us10y_mode={us10y.source_mode}")
    print(f"us10y_source_date={us10y.source_date}")
    print(f"upsert_changed={changed}")
    print(f"rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
