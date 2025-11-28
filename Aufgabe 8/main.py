from sparkstart import scon, spark
import ghcnd_stations
import matplotlib.pyplot as plt

# a) Liste aller Stationen sortiert nach Stationsname
def get_all_stations():
    result = spark.sql("""
        SELECT *
        FROM ghcndstations
        ORDER BY stationname
    """)
    result.show(truncate=False)


# b) Anzahl der Stationen je Land
def get_station_count_per_country():
    result = spark.sql("""
        SELECT
            c.countrycode,
            c.countryname,
            COUNT(s.stationid) AS count
        FROM ghcndstations s
        JOIN ghcndcountries c
            ON s.countrycode = c.countrycode
        GROUP BY
            c.countrycode,
            c.countryname
        ORDER BY count DESC
    """)
    result.show(truncate=False)


# c) Stationen in Deutschland
def get_german_stations():
    result = spark.sql("""
        SELECT *
        FROM ghcndstations
        WHERE countrycode = 'GM'
        ORDER BY stationname
    """)
    result.show(truncate=False)


# d) Plot TMAX und TMIN für Station und Jahr
def plot_temp_day(station_name, year):
    df_filtered = spark.sql(f"""
        SELECT
            d.date,
            d.value / 10.0 AS temp,
            d.element
        FROM ghcnddata d
        JOIN ghcndstations s
            ON d.stationid = s.stationid
        WHERE
            trim(upper(s.stationname)) = '{station_name.upper()}'
            AND year(d.date) = {year}
            AND d.element IN ('TMAX', 'TMIN')
        ORDER BY d.date
    """).collect()

    if not df_filtered:
        print(f"Keine Daten für Station '{station_name}' im Jahr {year} (oder Station nicht gefunden).")
        return

    # Daten in Dicts organisieren
    tmax_data = {row['date']: row['temp'] for row in df_filtered if row['element'] == 'TMAX'}
    tmin_data = {row['date']: row['temp'] for row in df_filtered if row['element'] == 'TMIN'}

    # Sortieren nach Datum
    dates = sorted(set(tmax_data.keys()) | set(tmin_data.keys()))
    tmax_vals = [tmax_data.get(d, None) for d in dates]
    tmin_vals = [tmin_data.get(d, None) for d in dates]
    day_of_year = [d.timetuple().tm_yday for d in dates]

    plt.plot(day_of_year, tmax_vals, 'r', label='TMAX')
    plt.plot(day_of_year, tmin_vals, 'b', label='TMIN')
    plt.xlabel('Tag des Jahres')
    plt.ylabel('Temperatur (°C)')
    plt.title(f'{station_name} {year}')
    plt.legend()
    plt.show()


# e) Gesamt-Niederschlag pro Jahr für Station
def plot_precip_year(station_name):
    df_precip = spark.sql(f"""
        SELECT
            year(d.date) AS year,
            SUM(d.value) / 10.0 AS total_precip
        FROM ghcnddata d
        JOIN ghcndstations s
            ON d.stationid = s.stationid
        WHERE
            trim(upper(s.stationname)) = '{station_name.upper()}'
            AND d.element = 'PRCP'
        GROUP BY year(d.date)
        ORDER BY year
    """).collect()

    if not df_precip:
        print(f"Keine Niederschlagsdaten für Station '{station_name}'.")
        return

    years = [row['year'] for row in df_precip]
    total_precip = [row['total_precip'] for row in df_precip]

    plt.bar(years, total_precip)
    plt.xlabel('Jahr')
    plt.ylabel('Niederschlag (mm)')
    plt.title(f'Gesamt-Niederschlag {station_name}')
    plt.show()


# f) Durchschnitt TMAX pro Tag des Jahres, mit 21-Tage Durchschnitt
def plot_avg_tmax_day(station_name):
    df_avg = spark.sql(f"""
        SELECT
            dayofyear(d.date) AS day,
            AVG(d.value) / 10.0 AS avg_tmax
        FROM ghcnddata d
        JOIN ghcndstations s
            ON d.stationid = s.stationid
        WHERE
            trim(upper(s.stationname)) = '{station_name.upper()}'
            AND d.element = 'TMAX'
        GROUP BY dayofyear(d.date)
        ORDER BY day
    """).collect()

    if not df_avg:
        print(f"Keine TMAX-Daten für Station '{station_name}'.")
        return

    days = [row['day'] for row in df_avg]
    avg_tmax = [row['avg_tmax'] for row in df_avg]
    #TODO: Mit SQL machen
    # 21-Tage gleitender Durchschnitt (10 Tage davor, Tag selbst, 10 Tage danach)
    rolling_avg = []
    for i in range(len(avg_tmax)):
        start = max(0, i - 10)
        end = min(len(avg_tmax), i + 11)
        rolling_avg.append(sum(avg_tmax[start:end]) / (end - start))

    plt.plot(days, avg_tmax, label='Täglich')
    plt.plot(days, rolling_avg, label='21-Tage')
    plt.xlabel('Tag des Jahres')
    plt.ylabel('Durchschnitt TMAX (°C)')
    plt.title(f'Durchschnitt TMAX {station_name}')
    plt.legend()
    plt.show()


# g) Durchschnitt TMAX und TMIN pro Jahr für Station
def plot_temp_year(station_name):
    df_temp = spark.sql(f"""
        SELECT
            year(d.date) AS year,
            AVG(CASE WHEN d.element = 'TMAX' THEN d.value END) / 10.0 AS avg_tmax,
            AVG(CASE WHEN d.element = 'TMIN' THEN d.value END) / 10.0 AS avg_tmin
        FROM ghcnddata d
        JOIN ghcndstations s
            ON d.stationid = s.stationid
        WHERE
            trim(upper(s.stationname)) = '{station_name.upper()}'
            AND d.element IN ('TMAX', 'TMIN')
        GROUP BY year(d.date)
        ORDER BY year
    """).collect()

    if not df_temp:
        print(f"Keine Temperaturdaten für Station '{station_name}'.")
        return

    years = [row['year'] for row in df_temp]
    avg_tmax = [row['avg_tmax'] for row in df_temp]
    avg_tmin = [row['avg_tmin'] for row in df_temp]

    plt.plot(years, avg_tmax, 'r', label='TMAX')
    plt.plot(years, avg_tmin, 'b', label='TMIN')
    plt.xlabel('Jahr')
    plt.ylabel('Temperatur (°C)')
    plt.title(f'Temperatur {station_name}')
    plt.legend()
    plt.show()


# h) Durchschnitt TMAX pro Jahr und 20-Jahre Durchschnitt
def plot_tmax_trend(station_name):
    df_trend = spark.sql(f"""
        SELECT
            year(d.date) AS year,
            AVG(d.value) / 10.0 AS avg_tmax
        FROM ghcnddata d
        JOIN ghcndstations s
            ON d.stationid = s.stationid
        WHERE
            trim(upper(s.stationname)) = '{station_name.upper()}'
            AND d.element = 'TMAX'
        GROUP BY year(d.date)
        ORDER BY year
    """).collect()

    if not df_trend:
        print(f"Keine TMAX-Daten für Station '{station_name}'.")
        return

    years = [row['year'] for row in df_trend]
    avg_tmax = [row['avg_tmax'] for row in df_trend]

    rolling_avg = []
    for i in range(len(avg_tmax)):
        start = max(0, i - 9)
        end = min(len(avg_tmax), i + 11)
        rolling_avg.append(sum(avg_tmax[start:end]) / (end - start))

    plt.plot(years, avg_tmax, label='Jährlich')
    plt.plot(years, rolling_avg, label='20-Jahre')
    plt.xlabel('Jahr')
    plt.ylabel('Durchschnitt TMAX (°C)')
    plt.title(f'TMAX Trend {station_name}')
    plt.legend()
    plt.show()


# i) Korrelation TMIN und TMAX pro Jahr
def plot_corr_temp(station_name):
    df_corr = spark.sql(f"""
        SELECT
            year(date) AS year,
            corr(tmin_val, tmax_val) AS correlation
        FROM (
            SELECT
                d.date,
                MAX(CASE WHEN d.element = 'TMIN' THEN d.value END) AS tmin_val,
                MAX(CASE WHEN d.element = 'TMAX' THEN d.value END) AS tmax_val
            FROM ghcnddata d
            JOIN ghcndstations s
                ON d.stationid = s.stationid
            WHERE
                trim(upper(s.stationname)) = '{station_name.upper()}'
                AND d.element IN ('TMIN', 'TMAX')
            GROUP BY d.date
        )
        GROUP BY year(date)
        ORDER BY year
    """).collect()

    if not df_corr:
        print(f"Keine Korrelationsdaten für Station '{station_name}'.")
        return

    years = [row['year'] for row in df_corr]
    correlation = [row['correlation'] for row in df_corr]

    plt.plot(years, correlation)
    plt.xlabel('Jahr')
    plt.ylabel('Korrelation TMIN-TMAX')
    plt.title(f'Korrelation {station_name}')
    plt.show()


def main(scon, spark):
    # Daten laden
    ghcnd_stations.read_ghcnd_from_parquet(spark)

    # a) Liste aller Stationen
    print("a)")
    get_all_stations()

    # b) Anzahl Stationen je Land
    print("b)")
    get_station_count_per_country()

    # c) Stationen in Deutschland
    print("c)")
    get_german_stations()

    # d) Plot für Kempten, Hohenpeissenberg, Zugspitze
    print("d)")
    plot_temp_day('KEMPTEN', 2024)
    plot_temp_day('HOHENPEISSENBERG', 2024)
    plot_temp_day('ZUGSPITZE', 2024)

    # e) Niederschlag
    print("e)")
    plot_precip_year('KEMPTEN')
    plot_precip_year('HOHENPEISSENBERG')
    plot_precip_year('ZUGSPITZE')

    # f) Durchschnitt TMAX
    print("f)")
    plot_avg_tmax_day('KEMPTEN')
    plot_avg_tmax_day('HOHENPEISSENBERG')
    plot_avg_tmax_day('ZUGSPITZE')

    # g) Temperatur pro Jahr
    print("g)")
    plot_temp_year('KEMPTEN')
    plot_temp_year('HOHENPEISSENBERG')
    plot_temp_year('ZUGSPITZE')

    # h) TMAX Trend
    print("h)")
    plot_tmax_trend('KEMPTEN')
    plot_tmax_trend('HOHENPEISSENBERG')
    plot_tmax_trend('ZUGSPITZE')

    # i) Korrelation
    print("i)")
    plot_corr_temp('KEMPTEN')
    plot_corr_temp('HOHENPEISSENBERG')
    plot_corr_temp('ZUGSPITZE')


if __name__ == "__main__":
    main(scon, spark)
