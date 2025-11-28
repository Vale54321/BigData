from sparkstart import scon, spark

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType
from pyspark.sql import Row
import pyspark.sql.functions as F
import re

CDC_PATH = "/data/cdc/hourly/"
HDFS_HOME = "hdfs://193.174.205.250:54310/"


# a) Stationsdaten einlesen & als Parquet speichern
def a(scon, spark, path=CDC_PATH):
    stationlines = scon.textFile(path + "TU_Stundenwerte_Beschreibung_Stationen.txt")

    stationlines = stationlines.zipWithIndex().filter(lambda x: x[1] >= 2).map(lambda x: x[0])

    stationsplitlines = stationlines.map(lambda l: (
        l[0:5].strip(),
        l[6:14].strip(),
        l[15:23].strip(), 
        int(l[24:41].strip()),
        float(l[42:52].strip()),
        float(l[53:61].strip()), 
        l[61:101].strip(), 
        l[102:].strip()
    ))
    
    stationschema = StructType([
        StructField('stationid', StringType(), True),
        StructField('from_date', StringType(), True),
        StructField('to_date', StringType(), True),
        StructField('height', IntegerType(), True),
        StructField('latitude', FloatType(), True),
        StructField('longitude', FloatType(), True),
        StructField('stationname', StringType(), True),
        StructField('state', StringType(), True)
    ])

    stationframe = spark.createDataFrame(stationsplitlines, schema=stationschema)

    stationframe.createOrReplaceTempView("cdc_stations")

    outfile = HDFS_HOME + "/home/kramlingermike/" + "cdc_stations.parquet"
    stationframe.write.mode('overwrite').parquet(outfile)
    stationframe.cache()

# a) Beispielabfrage
def get_all_cdc_stations(spark):
    result = spark.sql(f"""
        SELECT *
        FROM cdc_stations
        ORDER BY stationname
    """)
    result.show(truncate=False)

# a) Beispielabfrage
def get_cdc_stations_per_state(spark):
    result = spark.sql(f"""
        SELECT
            state,
            COUNT(*) AS count
        FROM cdc_stations
        GROUP BY state
        ORDER BY count DESC
    """)
    result.show(truncate=False)

def b(scon, spark):
    lines = scon.textFile(CDC_PATH + "produkt*")
    
    lines = lines.filter(lambda line: not line.startswith("STATIONS_ID"))
    lines = lines.zipWithIndex().filter(lambda x: x[1] >= 0).map(lambda x: x[0])

    lines = lines.map(lambda l: l.split(";"))
    
    lines = lines.map(lambda s: (
    s[0].strip(),
    s[1].strip()[:8],  
    int(s[1].strip()[8:]),
    int(s[2].strip()),
    float(s[3].strip()),
    float(s[4].strip())
    ))
    
    schema = StructType([
    StructField("stationid", StringType(), True),
    StructField("date", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("qn_9", IntegerType(), True),
    StructField("tt_tu", FloatType(), True),
    StructField("rf_tu", FloatType(), True)
    ])
    

    df = spark.createDataFrame(lines, schema)

    df.createOrReplaceTempView("cdc_hourly")

    outfile = HDFS_HOME + "home/kramlingermike/" + "cdc_hourly.parquet"
    df.write.mode("overwrite").parquet(outfile)
    
def get_hourly_station(spark, stationid, limit=20):
    result = spark.sql(f"""
        SELECT *
        FROM cdc_hourly
        WHERE stationid = '{stationid}'
        ORDER BY date, hour
        LIMIT {limit}
    """)
    result.show(truncate=False)
    
def avg_temp_per_day(spark, stationid, limit=20):
    result = spark.sql(f"""
        SELECT date, ROUND(AVG(tt_tu),2) AS avg_temp
        FROM cdc_hourly
        WHERE stationid = '{stationid}'
        GROUP BY date
        ORDER BY date
        LIMIT {limit}
    """)
    result.show(truncate=False)

    
def main(scon, spark):
    """
    main(scon, spark)
    """
    
    print("a)")
    a(scon, spark)
    print("Beispielabfrage: (Alle Stationen:)")
    get_all_cdc_stations(spark)
    print("Beispielabfrage: (Alle Stationen pro Bundesland)")
    get_cdc_stations_per_state(spark)
    print("b)")
    b(scon, spark)
    print("Beispielabfrage: (Alle Daten für eine Station:)")
    get_hourly_station(spark, "4271")
    print("Beispielabfrage: (Durchschnittliche Temperatur pro Tag für eine Station:)")
    avg_temp_per_day(spark, "4271")

if __name__ == "__main__":
    main(scon, spark)
