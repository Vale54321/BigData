from sparkstart import scon, spark
from pyspark.sql import SparkSession
import time
import matplotlib.pyplot as plt

HDFSPATH = "hdfs://193.174.205.250:54310/"


def read_parquets(spark: SparkSession):
	stations_path = HDFSPATH + "home/heiserervalentin/german_stations.parquet"
	products_path = HDFSPATH + "home/heiserervalentin/german_stations_data.parquet"

	stations_df = spark.read.parquet(stations_path)
	stations_df.createOrReplaceTempView("german_stations")

	products_df = spark.read.parquet(products_path)
	products_df.createOrReplaceTempView("german_stations_data")

	stations_df.cache()
	products_df.cache()


def plot_all_stations(spark: SparkSession):
	q = "SELECT geo_laenge AS lon, geo_breite AS lat FROM german_stations WHERE geo_laenge IS NOT NULL AND geo_breite IS NOT NULL"
	df = spark.sql(q)

	pdf = df.toPandas()
	plt.figure(figsize=(8, 6))
	plt.scatter(pdf.lon, pdf.lat, s=6, color='red', marker='.')
	plt.xlabel('Longitude')
	plt.ylabel('Latitude')
	plt.title('All Stations (locations)')
	plt.tight_layout()
	plt.show()


def duration_circle_size(spark: SparkSession):
	q = (
		"SELECT stationId, geo_laenge AS lon, geo_breite AS lat, "
		"(CAST(SUBSTR(bis_datum,1,4) AS INT) - CAST(SUBSTR(von_datum,1,4) AS INT)) AS duration_years "
		"FROM german_stations "
		"WHERE TRIM(von_datum)<>'' AND TRIM(bis_datum)<>''"
	)
	df = spark.sql(q)

	pdf = df.toPandas()

	pdf['duration_years'] = pdf['duration_years'].fillna(0).astype(int)
	sizes = (pdf['duration_years'].clip(lower=0) + 1) * 6

	plt.figure(figsize=(8, 6))
	plt.scatter(pdf.lon, pdf.lat, s=sizes, alpha=0.6, c=pdf['duration_years'], cmap='viridis')
	plt.colorbar(label='Duration (years)')
	plt.xlabel('Longitude')
	plt.ylabel('Latitude')
	plt.title('Stations with duration (years) as marker size')
	plt.tight_layout()
	plt.show()


def compute_daily_and_yearly_frosts(spark: SparkSession):
	q_daily_max = (
		"SELECT stationId, date, SUBSTR(date,1,4) AS year, MAX(TT_TU) AS max_temp "
		"FROM german_stations_data "
		"GROUP BY stationId, date"
	)
	daily_max = spark.sql(q_daily_max)
	daily_max.createOrReplaceTempView('daily_max')

	# mark a day as frost if max_temp < 0
	q_daily_frost = (
		"SELECT stationId, year, CASE WHEN max_temp < 0 THEN 1 ELSE 0 END AS is_frost "
		"FROM daily_max"
	)
	daily_frost = spark.sql(q_daily_frost)
	daily_frost.createOrReplaceTempView('daily_frost')

	# yearly frostdays per station
	q_station_year = (
		"SELECT stationId, year, SUM(is_frost) AS frost_days "
		"FROM daily_frost GROUP BY stationId, year"
	)
	station_year_frost = spark.sql(q_station_year)
	station_year_frost.createOrReplaceTempView('station_year_frost')


def frost_analysis(spark: SparkSession, year=2024, station_name_matches=('kempten',)):
	compute_daily_and_yearly_frosts(spark)

	q_hist = (
		f"SELECT frost_days, COUNT(*) AS station_count "
		f"FROM station_year_frost WHERE year = '{year}' GROUP BY frost_days ORDER BY frost_days"
	)
	hist_df = spark.sql(q_hist)

	hist_pdf = hist_df.toPandas()
	plt.figure(figsize=(8, 5))
	plt.bar(hist_pdf.frost_days, hist_pdf.station_count, color='steelblue')
	plt.xlabel('Number of Frost Days in year ' + str(year))
	plt.ylabel('Number of Stations')
	plt.title(f'Stations vs Frost Days ({year})')
	plt.tight_layout()
	plt.show()

	for name in station_name_matches:
		q_find = f"SELECT stationId, station_name FROM german_stations WHERE lower(station_name) LIKE '%{name.lower()}%'"
		ids_df = spark.sql(q_find)
		ids = ids_df.collect()
		if not ids:
			print(f"No stations found matching '{name}'")
			continue
		for r in ids:
			sid = r['stationId']
			sname = r['station_name']
			print(f"Analyzing stationId={sid} name={sname}")

			# compute frostdays + 5-yr and 20-yr rolling averages using window frame
			q_ts = (
				"SELECT year, frost_days, "
				"AVG(frost_days) OVER (PARTITION BY stationId ORDER BY CAST(year AS INT) ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_5, "
				"AVG(frost_days) OVER (PARTITION BY stationId ORDER BY CAST(year AS INT) ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_20 "
				f"FROM station_year_frost WHERE stationId = {sid} ORDER BY CAST(year AS INT)"
			)
			ts_df = spark.sql(q_ts)

			pdf = ts_df.toPandas()
			if pdf.empty:
				print(f"No yearly frost data for station {sid}")
				continue

			pdf['year'] = pdf['year'].astype(int)
			plt.figure(figsize=(10, 5))
			plt.plot(pdf.year, pdf.frost_days, label='Frostdays (year)', marker='o')
			plt.plot(pdf.year, pdf.avg_5, label='5-year avg', linestyle='--')
			plt.plot(pdf.year, pdf.avg_20, label='20-year avg', linestyle=':')
			plt.xlabel('Year')
			plt.ylabel('Frost Days')
			plt.title(f'Frost Days over Years for {sname} (station {sid})')
			plt.legend()
			plt.tight_layout()
			plt.show()


def height_frost_correlation(spark: SparkSession):
	compute_daily_and_yearly_frosts(spark)

	q_corr = (
		"SELECT syf.year AS year, corr(s.hoehe, syf.frost_days) AS height_frost_corr "
		"FROM station_year_frost syf JOIN german_stations s ON syf.stationId = s.stationId "
		"GROUP BY syf.year ORDER BY CAST(syf.year AS INT)"
	)
	
	corr_df = spark.sql(q_corr)

	corr_pdf = corr_df.toPandas()

	corr_pdf = corr_pdf.dropna(subset=['height_frost_corr'])
	if corr_pdf.empty:
		print("No non-NaN correlation values found.")
		return

	corr_pdf['year'] = corr_pdf['year'].astype(int)
	plt.figure(figsize=(10, 5))
	plt.bar(corr_pdf.year, corr_pdf.height_frost_corr, color='orange')
	plt.xlabel('Year')
	plt.ylabel('Correlation (height vs frostdays)')
	plt.title('Yearly correlation: station height vs number of frost days')
	plt.tight_layout()
	plt.show()


def main(scon, spark):
	read_parquets(spark)

	plot_all_stations(spark)

	duration_circle_size(spark)

	frost_analysis(spark, year=2024, station_name_matches=('kempten',))

	height_frost_correlation(spark)


if __name__ == '__main__':
	main(scon, spark)

