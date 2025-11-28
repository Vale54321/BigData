from sparkstart import scon, spark
from pyspark import SparkContext, rdd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType

import matplotlib.pyplot as plt 

HDFSPATH = "hdfs://193.174.205.250:54310/"
GHCNDPATH = HDFSPATH + "ghcnd/"
GHCNDHOMEPATH = "/data/ghcnd/"


# Aufgabe 9 a

def import_data(spark: SparkSession, scon: SparkContext):
    """
    %time import_data(spark, scon)
    """
    
    # Daten in RDD einlesen
    rdd_station = scon.textFile("/data/cdc/hourly/TU_Stundenwerte_Beschreibung_Stationen.txt")
    
    # Entfernen der ersten beiden Zeilen (Header und Trennzeile)
    rdd_station_filterd = (rdd_station
                           .zipWithIndex() # jede Zeile bekommt idx
                           .filter(lambda x: x[1] >= 2) # nur Zeilen mit idx >= 2 behalten
                           .map(lambda x: x[0])) # idx wieder entfernen

    
    rdd_station_splitlines = rdd_station_filterd.map(
        lambda l: (
            int(l[:6].strip()),              # Station ID
            l[6:15],                    # von_datum
            l[15:24],                   # bis_datum
            float(l[24:40].strip()),    # stations höhe
            float(l[40:53].strip()),    # geoBreite
            float(l[53:61].strip()),    # geoHöhe
            l[61:142],                  # Stationsname
            l[142:-1]                  # Bundesland
            ))
    
    # Datenschema festlegen
    stationschema = StructType(
        [
            StructField("stationId", IntegerType(), True),
            StructField("von_datum", StringType(), True),
            StructField("bis_datum", StringType(), True),
            StructField("hoehe", FloatType(), True),
            StructField("geo_breite", FloatType(), True),
            StructField("geo_laenge", FloatType(), True),
            StructField("station_name", StringType(), True),
            StructField("bundesland", StringType(), True)
        ]
    )
    
    # Data Frame erzeugen
    stationframe = spark.createDataFrame(rdd_station_splitlines, schema=stationschema)
    stationframe.printSchema()
    
    # Temporäre View erzeugen
    stationframe.createOrReplaceTempView("german_stations")
    
    # Data Frame in HDFS speichern
    stationframe.write.mode("overwrite").parquet(
        HDFSPATH + "home/heiserervalentin/german_stations.parquet"
        )
    
   
    
def read_data_from_parquet(spark):
    """
    read_data_from_parquet(spark)
    """
    df = spark.read.parquet(HDFSPATH + "home/heiserervalentin/german_stations.parquet")
    df.createOrReplaceTempView("german_stations")
    df.cache()
    
def sql_querys(spark):
    """
    sql_querys(spark)
    """
    spark.sql("SELECT * FROM german_stations").show(5, truncate=False)
    spark.sql("SELECT COUNT(*) AS Anzahl FROM german_stations").show()
    spark.sql("SELECT MAX(geo_breite) FROM german_stations").show()
    df = spark.sql("SELECT * FROM german_stations").toPandas()
    
    plt.figure(figsize=[6,6])
    plt.scatter(df.geo_laenge, df.geo_breite, marker='.', color = 'r')
    
    plt.show()


def import_produkt_files(spark: SparkSession, scon: SparkContext, path='/data/cdc/hourly/'): 
    """
    import_produkt_files(spark, scon)
    """
    
    # Daten in RDD einlesen
    rdd_produkt = scon.textFile(f"{path}/produkt*")  
    
    # Kopfzeile und Leerzeichen filtern
    rdd_filterd = rdd_produkt \
                    .filter(lambda l: l != 'STATIONS_ID;MESS_DATUM;QN_9;TT_TU;RF_TU;eor') \
                    .map(lambda l: [x.strip() for x in l.split(';')])
    
    # Zeilen in Felder aufteilen
    rdd_produkt_splitlines = rdd_filterd.map(
        lambda l: (
            int(l[0]),                        # Stat_id
            l[1][:8],                    # Messdatum
            int(l[1][8:10]),             # Messstunde
            int(l[2]),                   # Qualitätsniveau 
            float(l[3]),                 # Lufttemp.
            float(l[4]),                 # rel. Luftfeuchte
            int(l[1][0:4])               # jahr
            )
        )
    
    print(rdd_produkt_splitlines.take(5))
    
    # Datenschema definieren
    product_schema = StructType(
        [
            StructField("stationId", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("hour", IntegerType(), True),
            StructField("QN_9", IntegerType(), True),
            StructField("TT_TU", FloatType(), True),
            StructField("RF_TU", FloatType(), True),
            StructField("jahr", IntegerType(), True)
        ]
    )
    
    product_frame = spark.createDataFrame(rdd_produkt_splitlines, schema=product_schema)
    product_frame.printSchema()
    product_frame.createOrReplaceTempView("german_stations_data")
    

    product_frame.write.mode("overwrite").parquet(
        HDFSPATH + "home/heiserervalentin/german_stations_data.parquet"
        )
    
   
    
def read_product_data_from_parquet(spark):
    """
    read_product_data_from_parquet(spark)
    """
    df = spark.read.parquet(HDFSPATH + "home/heiserervalentin/german_stations_data.parquet")
    df.createOrReplaceTempView("german_stations_data")
    df.cache()

def main(scon, spark):
    # Daten importieren
    import_data(spark, scon)
    read_data_from_parquet(spark)
    sql_querys(spark)
    
    import_produkt_files(spark, scon)
    read_product_data_from_parquet(spark)

if __name__ == "__main__":
    main(scon, spark)