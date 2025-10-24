import math
import time
from typing import Iterable, List, Tuple, Dict

import numpy as np
import matplotlib.pyplot as plt

def stats_min_max_avg_count_sum(numbers: Iterable[float]) -> Tuple[float, float, float, int, float]:
    nums = list(numbers)
    count = len(nums)
    if count == 0:
        raise ValueError("Leere Eingabeliste nicht erlaubt")
    s = sum(nums)
    return (min(nums), max(nums), s / count, count, s)


def catalan_list(n: int) -> List[int]:
    if n < 0:
        raise ValueError("n muss >= 0 sein")
    c = [0] * (n + 1)
    c[0] = 1
    for i in range(1, n + 1):
        acc = 0
        for k in range(i):
            acc += c[k] * c[i - 1 - k]
        c[i] = acc
    return c


def read_adsb_lonlat_by_aircraft(path: str) -> Dict[str, Tuple[List[float], List[float]]]:
    flights = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            parts = [p.strip() for p in raw.split(",")] if "," in raw else raw.split()
            if len(parts) < 6:
                continue
            ac_id = parts[0]
            try:
                lat = float(parts[4])
                lon = float(parts[5])
            except ValueError:
                continue
            lons, lats = flights.setdefault(ac_id, ([], []))
            lons.append(lon)
            lats.append(lat)
    return flights


# ------------------------------
# Aufgaben
# ------------------------------

def aufgabe2_a() -> None:
    print("a) Arithmetik")
    vals = [
        7 + 5,
        7 - 5,
        7 * 5,
        7 / 5,
        7 % 5,
        7 ** 5,
    ]
    print(vals)


def aufgabe2_b() -> None:
    print("b) Listen")
    lst = [10, 20, 30, 40, 50]
    print("Start:", lst)
    print("Index 0:", lst[0], "Index -1:", lst[-1])
    print("Slice [1:4]:", lst[1:4])
    print("Slice [:3]:", lst[:3])
    print("Slice [2:]:", lst[2:])
    lst.insert(2, 999)
    print("Nach insert(2,999):", lst)
    del lst[3]
    print("Nach del lst[3]:", lst)
    lst.append(777)
    print("Nach append(777):", lst)


def aufgabe2_c() -> None:
    print("c) range-Objekt")
    total = 0
    for x in range(1000):
        total += x
    print("Summe 0..999:", total)


def aufgabe2_d() -> None:
    print("d) Dictionary")
    d = {"name": "Valentin", "semester": 1}
    print("Start:", d)
    print("name ->", d["name"])
    d["semester"] = d["semester"] + 1
    d["projekt"] = "Big Data"
    print("Geändert:", d)


def aufgabe2_e() -> None:
    print("e) Funktion min max avg count sum")
    res = stats_min_max_avg_count_sum(range(1000))
    print("(min, max, avg, count, sum) =", res)


def aufgabe2_f() -> None:
    print("f) List comprehension")
    nums = [x for x in range(1, 10001) if (x % 3 == 0 and x % 5 == 0 and x % 7 == 0)]
    print("Anzahl:", len(nums))
    print("Beispiele:", nums[:10])

    res = stats_min_max_avg_count_sum(nums)
    print("Stats:", res)


def aufgabe2_g() -> None:
    print("g) Sinus Cosinus Funktion")
    deg = np.linspace(0.0, 720.0, 5000)
    rad = np.deg2rad(deg)
    y_sin = np.sin(rad) + np.sin(2 * rad) / 2 + np.sin(4 * rad) / 4 + np.sin(8 * rad) / 8
    y_cos = np.cos(rad) + np.cos(2 * rad) / 2 + np.cos(4 * rad) / 4 + np.cos(8 * rad) / 8

    plt.figure()
    plt.plot(deg, y_sin, label="sin-Kombination")
    plt.plot(deg, y_cos, label="cos-Kombination")
    plt.xlabel("Winkel (Grad)")
    plt.ylabel("Funktionswert")
    plt.title("Sinus Cosinus Funktion")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def aufgabe2_h() -> None:
    print("h) Catalansche Zahlen")
    n = 20
    c = catalan_list(n)
    print(f"C0..C{n}:", c)


def aufgabe2_i() -> None:
    print("i) Laufzeitmessung Catalan(n) für n = 50..1500 (Schritt 50)")
    ns = list(range(50, 1501, 50))
    times_sec = []
    for n in ns:
        t0 = time.perf_counter()
        _ = catalan_list(n)
        t1 = time.perf_counter()
        times_sec.append(t1 - t0)

    # Scatterplot
    plt.figure()
    plt.scatter(ns, times_sec)
    plt.xlabel("n")
    plt.ylabel("Zeit [s]")
    plt.title("Catalan(n)  Laufzeit (Scatter)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Lineplot
    plt.figure()
    plt.plot(ns, times_sec)
    plt.xlabel("n")
    plt.ylabel("Zeit [s]")
    plt.title("Catalan(n)  Laufzeit (Linie)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def aufgabe2_j(path_adsb: str = "adsbprak.txt",
               path_adsb2: str = "adsbprak2.txt") -> None:
    print("j) ADSB-Daten: Pfade:", path_adsb, "und", path_adsb2)

    def plot_file(p: str) -> None:
        flights = read_adsb_lonlat_by_aircraft(p)
        if not flights:
            print(f"Keine gültigen Daten in: {p}")
            return

        plt.figure()
        for ac_id, (lons, lats) in flights.items():
            plt.plot(lons, lats, linewidth=1.0)
            plt.scatter(lons, lats, s=6)
        plt.xlabel("Länge (deg)")
        plt.ylabel("Breite (deg)")
        plt.title(f"ADSB-Flugbahnen (Linie + Punkte): {p}")
        plt.grid(True)
        plt.axis("equal")
        plt.tight_layout()
        plt.show()

    try:
        plot_file(path_adsb)
    except FileNotFoundError:
        print(f"Datei nicht gefunden: {path_adsb}")

    try:
        plot_file(path_adsb2)
    except FileNotFoundError:
        print(f"Datei nicht gefunden: {path_adsb2}")

if __name__ == "__main__":
    aufgabe2_a()
    print()
    aufgabe2_b()
    print()
    aufgabe2_c()
    print()
    aufgabe2_d()
    print()
    aufgabe2_e()
    print()
    aufgabe2_f()
    print()
    aufgabe2_g()
    print()
    aufgabe2_h()
    print()
    aufgabe2_i()
    print()
    aufgabe2_j()
