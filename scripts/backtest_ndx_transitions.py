#!/usr/bin/env python3
"""Backtest the 8-factor composite's regime transitions against Nasdaq100 forward returns.

Ports the exact indicator logic from index.html (equal weights, default directions,
thrG=+0.5 / thrR=-0.5) and evaluates two user hypotheses on Nasdaq100:

  H1 (turn weak):  Green -> not-Green  usually precedes a drop / correction.
  H2 (turn strong):Red   -> Range      often marks a bottom (turn up).

Usage:
  python3 scripts/backtest_ndx_transitions.py --csv data/8-factors.csv [--label adjusted]
"""
import argparse
import csv
import math
from statistics import mean, median

FACTORS = ["BTC_USD", "USD_Index", "TLT", "HYG", "US10Y_Yield", "VIX", "Nasdaq100", "SP500"]
# Baked default directions (as in index.html compute()): +1 = up-is-risk-on, -1 = up-is-risk-off
DEFAULT_DIR = {"BTC_USD": 1, "USD_Index": -1, "TLT": -1, "HYG": 1,
               "US10Y_Yield": 1, "VIX": -1, "Nasdaq100": 1, "SP500": 1}
THR_G, THR_R = 0.5, -0.5
HORIZONS = [5, 10, 20, 60]   # trading days
WARMUP = 252                 # skip degenerate warmup (SMA200 + 1Y VIX percentile)


def SMA(a, n, i):
    if i + 1 < n:
        return math.nan
    return sum(a[i - n + 1:i + 1]) / n


def pct(a, p, i):
    if i < p:
        return math.nan
    x = a[i - p]
    if x == 0:
        return math.nan
    return (a[i] - x) / x


def n_trend(arr, i, up=True):
    s200, s50 = SMA(arr, 200, i), SMA(arr, 50, i)
    p20, p60 = pct(arr, 20, i), pct(arr, 60, i)
    if up:
        A, B, C, D = arr[i] > s200, arr[i] > s50, p20 > 0, p60 > 0
    else:
        A, B, C, D = arr[i] < s200, arr[i] < s50, p20 < 0, p60 < 0
    s = (A + B + C + D) / 4
    return s * 2 - 1


def n_vix(arr, i):
    L = 252
    win = [v for v in arr[max(0, i - L + 1):i + 1] if math.isfinite(v)]
    if len(win) < 30:
        A, C, D = arr[i] < SMA(arr, 50, i), pct(arr, 20, i) < 0, pct(arr, 60, i) < 0
        return (A + C + D) / 3 * 2 - 1
    sv = sorted(win)
    r = 0
    while r < len(sv) and sv[r] <= arr[i]:
        r += 1
    p = r / len(sv)
    return max(-1.0, min(1.0, 1 - 2 * p))


def compute_states(rows):
    series = {f: [float(r[f]) for r in rows] for f in FACTORS}
    n = len(rows)
    w = 1.0 / len(FACTORS)
    states, fused_list = [], []
    for i in range(n):
        v = {}
        v["BTC_USD"] = n_trend(series["BTC_USD"], i, up=True)
        v["USD_Index"] = n_trend(series["USD_Index"], i, up=False)
        v["TLT"] = n_trend(series["TLT"], i, up=False)
        v["HYG"] = n_trend(series["HYG"], i, up=True)
        v["US10Y_Yield"] = n_trend(series["US10Y_Yield"], i, up=True)
        v["VIX"] = n_vix(series["VIX"], i)
        v["Nasdaq100"] = n_trend(series["Nasdaq100"], i, up=True)
        v["SP500"] = n_trend(series["SP500"], i, up=True)
        for k in v:
            if not math.isfinite(v[k]):
                v[k] = 0.0
        fused = sum(v[f] * w for f in FACTORS)
        st = "Green" if fused >= THR_G else ("Red" if fused <= THR_R else "Range")
        states.append(st)
        fused_list.append(fused)
    return states, fused_list


def fwd_ret(ndx, i, h):
    if i + h >= len(ndx):
        return None
    if ndx[i] == 0:
        return None
    return ndx[i + h] / ndx[i] - 1.0


def fwd_min_max(ndx, i, h):
    """min and max forward return within h trading days (path extremes)."""
    if ndx[i] == 0:
        return None, None
    lo = hi = 0.0
    end = min(len(ndx) - 1, i + h)
    for k in range(i + 1, end + 1):
        r = ndx[k] / ndx[i] - 1.0
        lo = min(lo, r)
        hi = max(hi, r)
    return lo, hi


def pctl(xs, q):
    if not xs:
        return float("nan")
    s = sorted(xs)
    idx = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[idx]


def summarize(name, idxs, ndx):
    print(f"\n=== {name}  (events={len(idxs)}) ===")
    for h in HORIZONS:
        rs = [fwd_ret(ndx, i, h) for i in idxs]
        rs = [r for r in rs if r is not None]
        if not rs:
            continue
        pos = sum(1 for r in rs if r > 0) / len(rs) * 100
        print(f"  +{h:>2}d: mean={mean(rs)*100:+6.2f}%  median={median(rs)*100:+6.2f}%  "
              f"%positive={pos:5.1f}%  p10={pctl(rs,0.1)*100:+6.2f}%  p90={pctl(rs,0.9)*100:+6.2f}%")
    # path extremes within 60d
    los = [fwd_min_max(ndx, i, 60)[0] for i in idxs]
    his = [fwd_min_max(ndx, i, 60)[1] for i in idxs]
    los = [x for x in los if x is not None]
    his = [x for x in his if x is not None]
    if los:
        print(f"  within 60d: avg max-drawdown={mean(los)*100:+6.2f}%  avg max-runup={mean(his)*100:+6.2f}%")


def baseline(ndx):
    print("\n=== BASELINE (all eligible days, unconditional) ===")
    n = len(ndx)
    for h in HORIZONS:
        rs = [fwd_ret(ndx, i, h) for i in range(WARMUP, n)]
        rs = [r for r in rs if r is not None]
        pos = sum(1 for r in rs if r > 0) / len(rs) * 100
        print(f"  +{h:>2}d: mean={mean(rs)*100:+6.2f}%  median={median(rs)*100:+6.2f}%  %positive={pos:5.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/8-factors.csv")
    ap.add_argument("--label", default="")
    ap.add_argument("--out-chart", default="")
    args = ap.parse_args()

    with open(args.csv, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    rows.sort(key=lambda r: r["Date"])
    states, fused = compute_states(rows)
    ndx = [float(r["Nasdaq100"]) for r in rows]
    dates = [r["Date"] for r in rows]
    n = len(rows)

    lose_green, exit_red = [], []
    enter_red, gain_green = [], []
    for i in range(1, n):
        if i < WARMUP:
            continue
        prev, cur = states[i - 1], states[i]
        if prev == "Green" and cur != "Green":
            lose_green.append(i)
        if prev == "Red" and cur == "Range":
            exit_red.append(i)
        if prev != "Red" and cur == "Red":
            enter_red.append(i)
        if prev != "Green" and cur == "Green":
            gain_green.append(i)

    lbl = f" [{args.label}]" if args.label else ""
    print("#" * 72)
    print(f"# Backtest{lbl}: {args.csv}")
    print(f"# rows={n}  range={dates[0]}..{dates[-1]}  (analysis from index {WARMUP} = {dates[WARMUP]})")
    print(f"# state counts: Green={states.count('Green')}  Range={states.count('Range')}  Red={states.count('Red')}")
    print("#" * 72)

    baseline(ndx)
    summarize("H1  Green -> not-Green  (turn weak, expect NEGATIVE fwd)", lose_green, ndx)
    summarize("H2  Red -> Range        (turn strong/bottom, expect POSITIVE fwd)", exit_red, ndx)
    # symmetric context
    summarize("(ctx) enter Red         (turn weak)", enter_red, ndx)
    summarize("(ctx) gain Green        (turn strong)", gain_green, ndx)

    # event dates for inspection
    def dts(idxs):
        return [dates[i] for i in idxs]
    print("\nlose_green dates:", ", ".join(dts(lose_green)))
    print("\nexit_red dates:", ", ".join(dts(exit_red)))

    if args.out_chart:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            from datetime import datetime
            xs = [datetime.strptime(d, "%Y-%m-%d") for d in dates]
            fig, ax = plt.subplots(figsize=(15, 6))
            ax.plot(xs, ndx, color="#1f2937", lw=0.9, label="Nasdaq100")
            ax.set_yscale("log")
            for i in lose_green:
                ax.axvline(xs[i], color="#ef4444", alpha=0.28, lw=1.0)
            for i in exit_red:
                ax.axvline(xs[i], color="#059669", alpha=0.32, lw=1.2)
            ax.scatter([xs[i] for i in lose_green], [ndx[i] for i in lose_green],
                       marker="v", color="#ef4444", s=42, zorder=5, label="Green->not-Green (turn weak)")
            ax.scatter([xs[i] for i in exit_red], [ndx[i] for i in exit_red],
                       marker="^", color="#059669", s=52, zorder=5, label="Red->Range (turn strong)")
            ax.set_title(f"Nasdaq100 vs composite regime transitions{lbl}")
            ax.legend(loc="upper left", fontsize=9)
            ax.grid(True, which="both", alpha=0.2)
            fig.tight_layout()
            fig.savefig(args.out_chart, dpi=120)
            print(f"\nchart saved: {args.out_chart}")
        except Exception as e:  # noqa: BLE001
            print(f"\n(chart skipped: {e})")


if __name__ == "__main__":
    main()
