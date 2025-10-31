# -*- coding: utf-8 -*-

"""
Erzeugen einer Spark-Konfiguration
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# connect to cluster
conf = SparkConf().setMaster("spark://193.174.205.250:7077").setAppName("HeisererValentin")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.executor.memory", '32g')
conf.set("spark.driver.memory", '8g')
conf.set("spark.cores.max", "40")
scon = SparkContext(conf=conf)


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
    .getOrCreate()
