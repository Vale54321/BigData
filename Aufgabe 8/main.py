from sparkstart import scon, spark
import ghcnd_stations
import matplotlib.pyplot as plt
import time

# a) Liste aller Stationen sortiert nach Stationsname
def get_all_stations():
    start = time.time()
    result = spark.sql("SELECT * FROM stations ORDER BY name")
    result.show()
    end = time.time()
    print(f"Zeit: {end - start}")
    # Zweite Ausführung
    start = time.time()
    result = spark.sql("SELECT * FROM stations ORDER BY name")
    result.show()
    end = time.time()
    print(f"Zeit zweite Ausführung: {end - start}")

# b) Anzahl der Stationen je Land
def get_station_count_per_country():
    start = time.time()
    result = spark.sql("""
        SELECT c.country_code, c.name, COUNT(s.id) as count 
        FROM stations s 
        JOIN ghcndcountries c ON s.country = c.country_code 
        GROUP BY c.country_code, c.name 
        ORDER BY count DESC
    """)
    result.show(truncate=False)
    end = time.time()
    print(f"Zeit: {end - start}")
    # Zweite
    start = time.time()
    result = spark.sql("""
        SELECT c.country_code, c.name, COUNT(s.id) as count 
        FROM stations s 
        JOIN ghcndcountries c ON s.country = c.country_code 
        GROUP BY c.country_code, c.name 
        ORDER BY count DESC
    """)
    result.show(truncate=False)
    end = time.time()
    print(f"Zeit zweite: {end - start}")

# c) Stationen in Deutschland
def get_german_stations():
    start = time.time()
    result = spark.sql("SELECT * FROM stations WHERE country = 'GM' ORDER BY name")
    result.show()
    end = time.time()
    print(f"Zeit: {end - start}")
    # Zweite
    start = time.time()
    result = spark.sql("SELECT * FROM stations WHERE country = 'GM' ORDER BY name")
    result.show()
    end = time.time()
    print(f"Zeit zweite: {end - start}")

# d) Plot TMAX und TMIN für Station und Jahr
def plot_temp_day(station_name, year):
    # Station ID finden
    station_id = spark.sql(f"SELECT id FROM stations WHERE name = '{station_name}'").collect()[0][0]
    # Daten filtern
    df_filtered = spark.sql(f"""
        SELECT date, TMAX, TMIN FROM ghcnd_data 
        WHERE station = '{station_id}' AND year(date) = {year}
        ORDER BY date
    """).toPandas()
    # Temperaturen in Grad umrechnen
    df_filtered['TMAX'] /= 10
    df_filtered['TMIN'] /= 10
    # Tage des Jahres
    df_filtered['day_of_year'] = df_filtered['date'].dt.dayofyear
    plt.plot(df_filtered['day_of_year'], df_filtered['TMAX'], 'r', label='TMAX')
    plt.plot(df_filtered['day_of_year'], df_filtered['TMIN'], 'b', label='TMIN')
    plt.xlabel('Tag des Jahres')
    plt.ylabel('Temperatur (°C)')
    plt.title(f'{station_name} {year}')
    plt.legend()
    plt.show()

# e) Gesamt-Niederschlag pro Jahr für Station
def plot_precip_year(station_name):
    station_id = spark.sql(f"SELECT id FROM stations WHERE name = '{station_name}'").collect()[0][0]
    df_precip = spark.sql(f"""
        SELECT year(date) as year, SUM(PRCP)/10 as total_precip 
        FROM ghcnd_data 
        WHERE station = '{station_id}' 
        GROUP BY year(date) 
        ORDER BY year
    """).toPandas()
    plt.bar(df_precip['year'], df_precip['total_precip'])
    plt.xlabel('Jahr')
    plt.ylabel('Niederschlag (mm)')
    plt.title(f'Gesamt-Niederschlag {station_name}')
    plt.show()

# f) Durchschnitt TMAX pro Tag des Jahres, mit 21-Tage Durchschnitt
def plot_avg_tmax_day(station_name):
    station_id = spark.sql(f"SELECT id FROM stations WHERE name = '{station_name}'").collect()[0][0]
    df_avg = spark.sql(f"""
        SELECT dayofyear(date) as day, AVG(TMAX)/10 as avg_tmax 
        FROM ghcnd_data 
        WHERE station = '{station_id}' 
        GROUP BY dayofyear(date) 
        ORDER BY day
    """).toPandas()
    # 21-Tage Durchschnitt
    df_avg['rolling_avg'] = df_avg['avg_tmax'].rolling(21, center=True).mean()
    plt.plot(df_avg['day'], df_avg['avg_tmax'], label='Täglich')
    plt.plot(df_avg['day'], df_avg['rolling_avg'], label='21-Tage')
    plt.xlabel('Tag des Jahres')
    plt.ylabel('Durchschnitt TMAX (°C)')
    plt.title(f'Durchschnitt TMAX {station_name}')
    plt.legend()
    plt.show()

# g) Durchschnitt TMAX und TMIN pro Jahr für Station
def plot_temp_year(station_name):
    station_id = spark.sql(f"SELECT id FROM stations WHERE name = '{station_name}'").collect()[0][0]
    df_temp = spark.sql(f"""
        SELECT year(date) as year, AVG(TMAX)/10 as avg_tmax, AVG(TMIN)/10 as avg_tmin 
        FROM ghcnd_data 
        WHERE station = '{station_id}' 
        GROUP BY year(date) 
        ORDER BY year
    """).toPandas()
    plt.plot(df_temp['year'], df_temp['avg_tmax'], 'r', label='TMAX')
    plt.plot(df_temp['year'], df_temp['avg_tmin'], 'b', label='TMIN')
    plt.xlabel('Jahr')
    plt.ylabel('Temperatur (°C)')
    plt.title(f'Temperatur {station_name}')
    plt.legend()
    plt.show()

# h) Durchschnitt TMAX pro Jahr und 20-Jahre Durchschnitt
def plot_tmax_trend(station_name):
    station_id = spark.sql(f"SELECT id FROM stations WHERE name = '{station_name}'").collect()[0][0]
    df_trend = spark.sql(f"""
        SELECT year(date) as year, AVG(TMAX)/10 as avg_tmax 
        FROM ghcnd_data 
        WHERE station = '{station_id}' 
        GROUP BY year(date) 
        ORDER BY year
    """).toPandas()
    # 20-Jahre Durchschnitt
    df_trend['rolling_avg'] = df_trend['avg_tmax'].rolling(20, center=True).mean()
    plt.plot(df_trend['year'], df_trend['avg_tmax'], label='Jährlich')
    plt.plot(df_trend['year'], df_trend['rolling_avg'], label='20-Jahre')
    plt.xlabel('Jahr')
    plt.ylabel('Durchschnitt TMAX (°C)')
    plt.title(f'TMAX Trend {station_name}')
    plt.legend()
    plt.show()

# i) Korrelation TMIN und TMAX pro Jahr
def plot_corr_temp(station_name):
    station_id = spark.sql(f"SELECT id FROM stations WHERE name = '{station_name}'").collect()[0][0]
    df_corr = spark.sql(f"""
        SELECT year(date) as year, corr(TMIN, TMAX) as correlation 
        FROM (
            SELECT date, TMIN, TMAX 
            FROM ghcnd_data 
            WHERE station = '{station_id}' AND TMIN IS NOT NULL AND TMAX IS NOT NULL
        ) 
        GROUP BY year(date) 
        ORDER BY year
    """).toPandas()
    plt.plot(df_corr['year'], df_corr['correlation'])
    plt.xlabel('Jahr')
    plt.ylabel('Korrelation TMIN-TMAX')
    plt.title(f'Korrelation {station_name}')
    plt.show()

def main(scon, spark):
    # Daten laden
    ghcnd_stations.read_ghcnd_from_parquet(spark)
    
    # a) Liste aller Stationen
    get_all_stations()
    
    # b) Anzahl Stationen je Land
    get_station_count_per_country()
    
    # c) Stationen in Deutschland
    get_german_stations()
    
    # d) Plot für Kempten, Hohenpeissenberg, Zugspitze
    plot_temp_day('KEMPTEN', 2020)
    plot_temp_day('HOHENPEISSENBERG', 2020)
    plot_temp_day('ZUGSPITZE', 2020)
    
    # e) Niederschlag
    plot_precip_year('KEMPTEN')
    plot_precip_year('HOHENPEISSENBERG')
    plot_precip_year('ZUGSPITZE')
    
    # f) Durchschnitt TMAX
    plot_avg_tmax_day('KEMPTEN')
    plot_avg_tmax_day('HOHENPEISSENBERG')
    plot_avg_tmax_day('ZUGSPITZE')
    
    # g) Temperatur pro Jahr
    plot_temp_year('KEMPTEN')
    plot_temp_year('HOHENPEISSENBERG')
    plot_temp_year('ZUGSPITZE')
    
    # h) TMAX Trend
    plot_tmax_trend('KEMPTEN')
    plot_tmax_trend('HOHENPEISSENBERG')
    plot_tmax_trend('ZUGSPITZE')
    
    # i) Korrelation
    plot_corr_temp('KEMPTEN')
    plot_corr_temp('HOHENPEISSENBERG')
    plot_corr_temp('ZUGSPITZE')

if __name__ == "__main__":
    main(scon, spark)