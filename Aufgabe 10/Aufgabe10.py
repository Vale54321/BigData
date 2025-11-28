from sparkstart import scon, spark
import ghcnd_stations
import matplotlib.pyplot as plt
import time

# a) Scatterplot: alle Stationen (lon/lat)
def plot_all_stations(spark):
    q = """
        SELECT stationname, latitude, longitude
        FROM ghcndstations
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    t0 = time.time()
    rows = spark.sql(q).collect()
    t1 = time.time()
    print(f"Ausfuehrungszeit (SQL): {t1 - t0:.3f}s -- Rows: {len(rows)}")

    lats = [r['latitude'] for r in rows]
    lons = [r['longitude'] for r in rows]
    names = [r['stationname'] for r in rows]

    plt.figure(figsize=(8,6))
    plt.scatter(lons, lats, s=10, alpha=0.6)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Alle GHCND-Stationen (Scatter)')
    plt.grid(True)
    plt.show()


# b) Scatterplot: Stationsdauer in Jahren als Marker-Size (aus ghcndinventory: firstyear/lastyear)
def plot_station_duration(spark, size_factor=20):
    q = """
        SELECT
            s.stationname,
            s.latitude,
            s.longitude,
            (COALESCE(i.lastyear, year(current_date())) - COALESCE(i.firstyear, year(current_date()))) AS years
        FROM ghcndstations s
        LEFT JOIN ghcndinventory i ON s.stationid = i.stationid
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
    """
    t0 = time.time()
    rows = spark.sql(q).collect()
    t1 = time.time()
    print(f"Ausfuehrungszeit (SQL): {t1 - t0:.3f}s -- Rows: {len(rows)}")

    lats = [r['latitude'] for r in rows]
    lons = [r['longitude'] for r in rows]
    years = [r['years'] if r['years'] is not None else 0 for r in rows]
    sizes = [max(5, (y+1) * size_factor) for y in years]

    plt.figure(figsize=(8,6))
    plt.scatter(lons, lats, s=sizes, alpha=0.6)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('GHCND-Stationen: Dauer der Verfuegbarkeit (Größe ~ Jahre)')
    plt.grid(True)
    plt.show()


def plot_frost_distribution_year(spark, year):
    q = f"""
        WITH daily_max AS (
            SELECT stationid, date, MAX(CAST(value AS DOUBLE))/10.0 AS max_temp
            FROM ghcnddata
            WHERE element = 'TMAX'
              AND length(date) >= 4
              AND substr(date,1,4) = '{year}'
            GROUP BY stationid, date
        ),
        station_frost AS (
            SELECT dm.stationid, SUM(CASE WHEN dm.max_temp < 0 THEN 1 ELSE 0 END) AS frostdays
            FROM daily_max dm
            GROUP BY dm.stationid
        )
        SELECT sf.frostdays, COUNT(*) AS stations
        FROM station_frost sf
        GROUP BY sf.frostdays
        ORDER BY sf.frostdays
    """
    t0 = time.time()
    rows = spark.sql(q).collect()
    t1 = time.time()
    print(f"Ausfuehrungszeit (SQL): {t1 - t0:.3f}s -- Distinct frostdays: {len(rows)}")

    if not rows:
        print(f"Keine Daten f\u00fcr Jahr {year}.")
        return

    x = [r['frostdays'] for r in rows]
    y = [r['stations'] for r in rows]

    plt.figure(figsize=(8,5))
    plt.bar(x, y)
    plt.xlabel('Anzahl Frosttage im Jahr ' + str(year))
    plt.ylabel('Anzahl Stationen')
    plt.title(f'Verteilung der Frosttage pro Station im Jahr {year}')
    plt.grid(True)
    plt.show()


# c2) Frosttage Zeitreihe für eine Station mit 5- und 20-Jahres Durchschnitt (SQL window)
def plot_station_frost_timeseries(spark, station_name):
    q = f"""
        WITH daily_max AS (
            SELECT stationid, date, MAX(CAST(value AS DOUBLE))/10.0 AS max_temp
            FROM ghcnddata
            WHERE element = 'TMAX'
            GROUP BY stationid, date
        ),
        yearly AS (
            SELECT
                dm.stationid,
                CAST(substr(dm.date,1,4) AS INT) AS year,
                SUM(CASE WHEN dm.max_temp < 0 THEN 1 ELSE 0 END) AS frostdays
            FROM daily_max dm
            GROUP BY dm.stationid, CAST(substr(dm.date,1,4) AS INT)
        ),
        station_yearly AS (
            SELECT
                y.year,
                y.frostdays,
                AVG(y.frostdays) OVER (ORDER BY y.year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg5,
                AVG(y.frostdays) OVER (ORDER BY y.year ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg20
            FROM yearly y
            JOIN ghcndstations s ON y.stationid = s.stationid
            WHERE trim(upper(s.stationname)) = '{station_name.upper()}'
            ORDER BY y.year
        )
        SELECT * FROM station_yearly
    """
    t0 = time.time()
    rows = spark.sql(q).collect()
    t1 = time.time()
    print(f"Ausfuehrungszeit (SQL): {t1 - t0:.3f}s -- Years: {len(rows)}")

    if not rows:
        print(f"Keine Daten f\u00fcr Station '{station_name}'.")
        return

    years = [r['year'] for r in rows]
    frostdays = [r['frostdays'] for r in rows]
    avg5 = [r['avg5'] for r in rows]
    avg20 = [r['avg20'] for r in rows]

    plt.figure(figsize=(10,5))
    plt.plot(years, frostdays, label='Frosttage (Jahr)')
    plt.plot(years, avg5, label='5-Jahres-Durchschnitt')
    plt.plot(years, avg20, label='20-Jahres-Durchschnitt')
    plt.xlabel('Jahr')
    plt.ylabel('Anzahl Frosttage')
    plt.title(f'Frosttage f\u00fcr Station {station_name}')
    plt.legend()
    plt.grid(True)
    plt.show()


# d) Korrelation Hoehe (elevation) vs. Frosttage pro Jahr
def plot_height_frost_correlation(spark):
    q = """
        WITH daily_max AS (
            SELECT stationid, date, MAX(CAST(value AS DOUBLE))/10.0 AS max_temp
            FROM ghcnddata
            WHERE element = 'TMAX'
            GROUP BY stationid, date
        ),
        yearly AS (
            SELECT
                dm.stationid,
                CAST(substr(dm.date,1,4) AS INT) AS year,
                SUM(CASE WHEN dm.max_temp < 0 THEN 1 ELSE 0 END) AS frostdays
            FROM daily_max dm
            GROUP BY dm.stationid, CAST(substr(dm.date,1,4) AS INT)
        ),
        joined AS (
            SELECT y.year, s.elevation, y.frostdays
            FROM yearly y
            JOIN ghcndstations s ON y.stationid = s.stationid
            WHERE s.elevation IS NOT NULL
        ),
        yearly_corr AS (
            SELECT year, corr(elevation, frostdays) AS corr
            FROM joined
            GROUP BY year
            ORDER BY year
        )
        SELECT year, corr FROM yearly_corr WHERE corr IS NOT NULL
    """
    t0 = time.time()
    rows = spark.sql(q).collect()
    t1 = time.time()
    print(f"Ausfuehrungszeit (SQL): {t1 - t0:.3f}s -- Years with corr: {len(rows)}")

    if not rows:
        print("Keine Korrelationsdaten verfügbar.")
        return

    years = [r['year'] for r in rows]
    corr = [r['corr'] for r in rows]

    plt.figure(figsize=(10,5))
    plt.bar(years, corr)
    plt.xlabel('Jahr')
    plt.ylabel('Korrelationskoeffizient (elevation vs frostdays)')
    plt.title('Korrelation Hoehe (elevation) vs. Frosttage pro Jahr')
    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    ghcnd_stations.read_ghcnd_from_parquet(spark)

    plot_all_stations(spark)
    plot_station_duration(spark)
    plot_frost_distribution_year(spark, '2010')
    plot_station_frost_timeseries(spark, 'KEMPTEN')
    plot_height_frost_correlation(spark)
    pass
