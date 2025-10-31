import os
import re
import time
import argparse
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

from sparkstart import scon
import pandas as pd 
import matplotlib.pyplot as plt

def _safe_split(line: str) -> list:
    return line.rstrip("\n").split(",")


def count_unique_aircraft(
    sc,
    filepath: str,
    granularity: str = "hour",
    partitions: Optional[int] = None,
) -> Tuple[List[Tuple[datetime, int]], str]:
    assert granularity in {"hour", "day"}, "granularity must be 'hour' or 'day'"

    lines = sc.textFile(filepath)

    parts = lines.map(_safe_split)
    if partitions:
        parts = parts.coalesce(partitions)

    def valid_row(cols: list) -> bool:
        try:
            if len(cols) < 8:
                return False
            msg_type = cols[1].strip()
            return msg_type in {"2", "3"}
        except Exception:
            return False

    filtered = parts.filter(valid_row)

    def to_slot_aircraft(cols: list) -> Optional[Tuple[str, str]]:
        try:
            aircraft = cols[4].strip()
            date_s = cols[6].strip()
            time_s = cols[7].strip()
            if granularity == "day":
                slot = date_s
            else:  # hour
                hh = time_s.split(":")[0] if ":" in time_s else time_s[:2]
                if not hh:
                    return None
                slot = f"{date_s} {hh}:00"
            if not aircraft:
                return None
            return (slot, aircraft)
        except Exception:
            return None

    slot_aircraft = filtered.map(to_slot_aircraft).filter(lambda x: x is not None)

    unique_pairs = slot_aircraft.distinct()

    counts_by_slot = unique_pairs.map(lambda sa: (sa[0], 1)).reduceByKey(lambda a, b: a + b)

    sorted_counts = counts_by_slot.sortByKey(ascending=True)

    def to_dt_count(slot_count: Tuple[str, int]) -> Optional[Tuple[datetime, int]]:
        slot, cnt = slot_count
        try:
            if granularity == "day":
                dt = datetime.strptime(slot, "%Y/%m/%d")
            else:
                dt = datetime.strptime(slot, "%Y/%m/%d %H:%M")
            return (dt, int(cnt))
        except (ValueError, TypeError):
            return None

    dt_counts = sorted_counts.map(to_dt_count).filter(lambda x: x is not None).collect()

    if dt_counts:
        dt_counts.sort(key=lambda x: x[0])
        out_dir = os.path.dirname(__file__)
        df = pd.DataFrame({
            "datetime": [d for d, _ in dt_counts],
            "count": [c for _, c in dt_counts],
        })
        df = df.sort_values("datetime")
        plt.figure(figsize=(10, 4))
        plt.plot(df["datetime"], df["count"], marker="o", linestyle="-", color="tab:blue")
        plt.title(f"Distinct aircraft per {granularity}")
        plt.xlabel("Time")
        plt.ylabel("# Aircraft")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

    return dt_counts


def benchmark_partitions(
    sc,
    filepath: str,
    granularity: str = "hour",
    partition_list: Iterable[int] = (5, 50, 500),
) -> List[Tuple[int, float]]:
    results: List[Tuple[int, float]] = []
    for p in partition_list:
        t0 = time.perf_counter()
        _ = count_unique_aircraft(sc, filepath, granularity=granularity, partitions=p)
        dt = time.perf_counter() - t0
        print(f"Partitionen={p}: Laufzeit {dt:.2f} s")
        results.append((p, dt))

    # Save quick markdown table
    out_dir = os.path.dirname(__file__)
    md_path = os.path.join(out_dir, f"bench_partitions_{granularity}.md")
    try:
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("| Partitions | Wall time (s) |\n|---:|---:|\n")
            for p, dt in results:
                f.write(f"| {p} | {dt:.2f} |\n")
        print(f"Benchmark-Tabelle gespeichert: {md_path}")
    except Exception as e:
        print(f"Fehler beim Speichern der Benchmark-Tabelle: {e}")

    return results


def main():
    results = count_unique_aircraft(
        scon,
        "/data/adsb/adsb20221203.txt",
        granularity="day",
        partitions=1
    )

    print(f"Anzahl Zeit-Slots im Ergebnis: {len(results)}")
    if results:
        print("Erste 5 Ergebnisse:")
        for d, c in results[:5]:
            print(d.isoformat(), c)


if __name__ == "__main__":
    main()
