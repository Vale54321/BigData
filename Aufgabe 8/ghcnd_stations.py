#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load stations, countries, inventory and data from GHCND as Dataset.

@author: steger

"""

# pylint: disable=pointless-string-statement

import os
from datetime import date
from time import time
from subprocess import call
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType


# =============================================
# run sparkstart.py before to create a session
# =============================================

HDFSPATH = "hdfs://193.174.205.250:54310/"
GHCNDPATH = HDFSPATH + "ghcnd/"
GHCNDHOMEPATH = "/data/ghcnd/"


def conv_elevation(elev):
    """
    Convert an elevation value.

    -999.9 means there is no value.

    Parameters
    ----------
    elev : string
        The elevation to convert to float.

    Returns
    -------
    res : numeric
        The converted value as float.
    """
    elev = elev.strip()
    if elev == "-999.9":
        res = None
    else:
        res = float(elev)
    return res


def conv_data_value(line, start):
    """
    Convert a single data value from a dly.- File.

    Parameters
    ----------
    line  : string
        The line with the data value.
    start : int
        The index at which the value starts.

    Returns
    -------
    res : numeric
        The onverted data value as int.
    """
    return int(line[start:start+5].strip())


def import_ghcnd_stations(scon, spark, path):
    """
    Read the station data into a dataframe.

    Register it as temporary view and write it to parquet.

    Parameters
    ----------
    scon  : SparkContext
        The spark context.
    spark : SparkSession
        The SQL session.

    Returns
    -------
    stationFrame : DataFrame
        The spark Data Frame with the stations data.
    """
    stationlines = scon.textFile(path + "ghcnd-stations.txt")
    stationsplitlines = stationlines.map(
        lambda l:
        (l[0:2],
         l[2:3],
         l[0:11],
         float(l[12:20].strip()),
         float(l[21:30].strip()),
         conv_elevation(l[31:37]),
         l[41:71]
         ))
    stationschema = StructType([
        StructField('countrycode', StringType(), True),
        StructField('networkcode', StringType(), True),
        StructField('stationid',   StringType(), True),
        StructField('latitude',    FloatType(),  True),
        StructField('longitude',   FloatType(),  True),
        StructField('elevation',   FloatType(),  True),
        StructField('stationname', StringType(), True)
    ])
    stationframe = spark.createDataFrame(stationsplitlines,
                                         schema=stationschema)
    stationframe.createOrReplaceTempView("ghcndstations")
    stationframe.write.mode('overwrite').parquet(
        GHCNDPATH + "ghcndstations.parquet")
    stationframe.cache()
    print("Imported GhcndStations")
    return stationframe


def import_ghcnd_countries(scon, spark, path):
    """
    Read the countries data into a dataframe.

    Register it as temptable and write it to parquet.

    Parameters
    ----------
    scon  : SparkContext
        The spark context.
    spark : SparkSession
        The SQL session.
    path  : string
        The path where the file with data resides.

    Returns
    -------
    stationFrame : DataFrame
        The spark Data Frame with the countries data.
    """
    countrylines = scon.textFile(path + "ghcnd-countries.txt")
    countrysplitlines = countrylines.map(lambda l: (l[0:2], l[2:50]))
    countryschema = StructType([
        StructField('countrycode', StringType(), True),
        StructField('countryname', StringType(), True)])
    countryframe = spark.createDataFrame(countrysplitlines, countryschema)
    countryframe.createOrReplaceTempView("ghcndcountries")
    countryframe.write.mode('overwrite').parquet(
        GHCNDPATH + "ghcndcountries.parquet")
    countryframe.cache()
    print("Imported GhcndCountries")
    return countryframe


def conv_data_line(line):
    """
    Convert a data line from GHCND-Datafile (.dly).

    Parameters
    ----------
    line : string
        String with a data line containing the values for one month.

    Returns
    -------
    list of tuple
        List containing a tuple for each data value.
    """
    if line == '':
        return []

    countrycode = line[0:2]
    networkcode = line[2:3]
    stationid = line[0:11]
    year = int(line[11:15])
    month = int(line[15:17])
    element = line[17:21]
    datlst = []
    for i in range(0, 30):
        val = conv_data_value(line, 21 + i*8)
        if val != -9999:
            datlst.append((countrycode, networkcode, stationid,
                           year, month, i+1,
                           date(year, month, i+1),
                           element,
                           val))
    return datlst


def read_dly_file(scon, spark, filename):
    """
    Read a .dly-file into a data frame.

    Parameters
    ----------
    scon  : SparkContext
        The spark context.
    spark : SparkSession
        The SQL session.
    filename : string
        The name and path of the dly-File.

    Returns
    -------
    RDD
        The RDD with the contents of the dly-File.
    """
    dly = scon.textFile(filename)
    return process_dly_file_lines(spark, dly)


def process_dly_file_lines(spark, lines):
    """
    Process the lines of one dly file.

    Parameters
    ----------
    spark : SparkSession
        The SQL session.
    lines : RDD
        RDD with one value per line.

    Returns
    -------
    dlyFrame : DataFram
        Data Frame containing the data of the file.

    """
    dlsplit = lines.flatMap(conv_data_line)
    dlyfileschema = StructType([
        StructField('countrycode', StringType(),  True),
        StructField('networkcode', StringType(),  True),
        StructField('stationid',   StringType(),  True),
        StructField('year',        IntegerType(), True),
        StructField('month',       IntegerType(), True),
        StructField('day',         IntegerType(), True),
        StructField('date',        DateType(),    True),
        StructField('element',     StringType(),  True),
        StructField('value',       IntegerType(), True)
    ])
    dlyframe = spark.createDataFrame(dlsplit, dlyfileschema)
    return dlyframe


def import_data_rdd_parallel(scon, spark, path):
    """
    Import the data files from ghcnd in parallel.

    This is much faster on a cluster or a computer with many cores
    and enough main memory to hold all the raw data.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.

    Returns
    -------
    None.
    """
    rdd = scon.textFile(
        path+"/ghcnd_all/*.dly", minPartitions=5000)
    rddcoa = rdd.coalesce(5000)

    rddsplit = rddcoa.flatMap(conv_data_line)
    print("Number of data records = " + str(rddsplit.count()))
    print("Number of partitions = " + str(rddsplit.getNumPartitions()))

    dlyfileschema = StructType([
        StructField('countrycode', StringType(),  True),
        StructField('networkcode', StringType(),  True),
        StructField('stationid',   StringType(),  True),
        StructField('year',        IntegerType(), True),
        StructField('month',       IntegerType(), True),
        StructField('day',         IntegerType(), True),
        StructField('date',        DateType(),    True),
        StructField('element',     StringType(),  True),
        StructField('value',       IntegerType(), True)
    ])
    dlyframe = spark.createDataFrame(rddsplit, dlyfileschema)

    dlyframe.show(10)

    dlyframe.write.mode('overwrite').parquet(
        GHCNDPATH + "ghcnddata.parquet")
    print(os.system("hdfs dfs -du -s /ghcnd/ghcnddata.parquet"))


def import_data_rdd_parallel_whole(scon, spark, path):
    """
    Import the data files from ghcnd in parallel.

    This is much faster on a cluster or a computer with many cores
    and enough main memory to hold all the raw data.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.

    Returns
    -------
    None.
    """
    rdd = scon.wholeTextFiles(
        path+"/ghcnd_all/*.dly", minPartitions=5000	)

    rddvals = rdd.values()
    print("Number of files in GHCND = " + str(rddvals.count()))
    rddlen = rddvals.map(len)
    print("Number of characters in all files = " +
          str(rddlen.reduce(lambda x, y: x + y)))

    rddlines = rddvals.flatMap(lambda x: x.split("\n"))
    print("Number of lines with data = " + str(rddlines.count()))

    rddsplit = rddlines.flatMap(conv_data_line)
    print("Number of data records = " + str(rddsplit.count()))
    print("Number of partitions = " + str(rddsplit.getNumPartitions()))

    dlyfileschema = StructType([
        StructField('countrycode', StringType(),  True),
        StructField('networkcode', StringType(),  True),
        StructField('stationid',   StringType(),  True),
        StructField('year',        IntegerType(), True),
        StructField('month',       IntegerType(), True),
        StructField('day',         IntegerType(), True),
        StructField('date',        DateType(),    True),
        StructField('element',     StringType(),  True),
        StructField('value',       IntegerType(), True)
    ])
    dlyframe = spark.createDataFrame(rddsplit, dlyfileschema)

    dlyframe.show(10)

    dlyframe.write.mode('overwrite').parquet(
        GHCNDPATH + "ghcnddata.parquet")
    print(os.system("hdfs dfs -du -s /ghcnd/ghcnddata.parquet"))

    """
    Code for testing problems that resulted finally from empty lines
    to solve the problem the code
        if line == '':
        return []
    was added at the beginning of convDataLine to filter away empty lines:

    noyear = rddsplit.filter(lambda x: not x[3].isnumeric())
    noyear.collect()

    rddlines1 = rdd.flatMap(lambda x: [(x[0], y) for y in x[1].split("\n")])
    print(rddlines1.count())

    rddsplit1 = rddlines1.flatMap(convDataLine1)
    print(rddsplit1.count())

    noyear1 = rddsplit1.filter(lambda x: not x[1][3].isnumeric())
    noyear1.collect()
    """


def import_ghcnd_files_extern(scon, spark, path, stationlist, batchsize,
                              numparts):
    """
    Import multiple data files in one batch.

    Import batchsize data files in one batch and append the data into
    the parquet file.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.
    path : string
        Path of the data files.
    stationlist : list
        List of all stations to load.
    batchsize : int
        Number of files to load in one batch.
    numparts : int
        Number of partitions to write one batch.

    Returns
    -------
    None.

    """
    data = None
    count = 0
    allcount = 0
    batchcount = 0
    for station in stationlist:
        # filename = "file://" +  path + "/" + station + ".dly"
        filename = path + station + ".dly"
        if os.path.isfile(filename):
            dly = read_dly_file(spark, scon, "file://" + filename)
            if data is not None:
                data = data.union(dly)
                print("Batch " + str(batchcount) +
                      " Filenr " + str(count) + " Processing " + filename)
            else:
                tstart = time()
                data = dly
            count += 1
            if count >= batchsize:
                # data = data.sort('countrycode', 'stationid', 'date')
                data = data.coalesce(numparts)
                tcoalesce = time()
                data.write.mode('Append').parquet(
                    GHCNDPATH + "ghcnddata.parquet")
                anzrec = data.count()
                twrite = time()
                print(
                    "\n\nBatch " + str(batchcount) +
                    " #recs " + str(anzrec) +
                    " #files " + str(allcount) +
                    " readtime " + str.format("{:f}", tcoalesce - tstart) +
                    " writetime " + str.format("{:f}", twrite - tcoalesce) +
                    " recs/sec " +
                    str.format("{:f}", anzrec / (twrite - tstart)) + "\n\n")
                allcount += count
                count = 0
                batchcount += 1
                data = None
        else:
            print("importGhcndFilesExtern: " + station +
                  ", " + filename + "   not found")
    if data is not None:
        data = data.coalesce(numparts)
        data.write.mode('Append').parquet(GHCNDPATH + "ghcnddata.parquet")


def import_all_data(scon, spark, path):
    """
    Import all data from GHCND.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.
    path : string
        Path of data files.

    Returns
    -------
    None.

    """
    stationlist = spark.sql(
        "SELECT stationid AS station \
        FROM ghcndstations \
        ORDER BY station")
    pds = stationlist.toPandas()
    import_ghcnd_files_extern(scon, spark, path + "ghcnd_all/",
                              pds.station, 30, 1)


def import_data_single_files(scon, spark, stationlist, parquetname, path):
    """
    Import the data files one by one.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.
    stationlist : list
        List of all stations to import data.
    parquetname : string
        Name of the parquet file to write the data to.
    path : string
        Path where the data files reside.

    Returns
    -------
    None.

    """
    pds = stationlist.toPandas()
    cnt = 0
    for station in pds.station:
        filename = path + station + ".dly"
        if os.path.isfile(filename):
            start = time()
            dly = read_dly_file(spark, scon,  "file://" + filename)
            numrec = dly.count()
            dly = dly.coalesce(1).sort('element', 'date')
            read = time()
            dly.write.mode('Append').parquet(GHCNDPATH
                                             + parquetname + ".parquet")
            finish = time()
            print(str.format(
                "{:8d}    ", cnt) + station +
                " #rec " + str.format("{:7d}", numrec) +
                " read " + str.format("{:f}", read - start) +
                " write " + str.format("{:f}", finish - read) +
                " write/sec " + str.format("importDataSingleFiles{:f} ",
                                           numrec/(finish - read))
                + " " + filename)
        else:
            print("#### " + str(cnt) + " File " +
                  filename + " does not exist ####")
        cnt += 1


def check_files(spark):
    """
    Check if some files for generated stationnames do not exist.

    Parameters
    ----------
    spark : SparkSession
        The SQL session.

    Returns
    -------
    None.

    """
    stationlist = spark.sql(
        "SELECT CONCAT(countrycode, networkcode, stationid) AS station \
       FROM ghcndstations \
       ORDER BY station")
    pds = stationlist.toPandas()
    count = 1
    for station in pds.station:
        filename = "/nfs/home/steger/ghcnd/ghcnd_all/" + station + ".dly"
        if os.path.isfile(filename):
            # print(str(count) + "   " + station)
            pass
        else:
            print(str(count) + "  File does not exist: " + filename)
        count += 1

    """
    Read the inventory data into a dataframe,
    register it as temporary view and write it to parquet
    """


def import_ghcnd_inventory(scon, spark, path):
    """
    Import inventory information from GHCND.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.
    path : string
        Path for inventory file.

    Returns
    -------
    invframe : DataFrame
        Data Frame with inventory data.

    """
    invlines = scon.textFile(path + "ghcnd-inventory.txt")
    invsplitlines = invlines.map(
        lambda l:
        (l[0:2],
         l[2:3],
         l[0:11],
         float(l[12:20].strip()),
         float(l[21:30].strip()),
         l[31:35],
         int(l[36:40]),
         int(l[41:45])
         ))
    invschema = StructType([
        StructField('countrycode', StringType(), True),
        StructField('networkcode', StringType(), True),
        StructField('stationid',   StringType(), True),
        StructField('latitude',    FloatType(),  True),
        StructField('longitude',   FloatType(),  True),
        StructField('element',     StringType(),  True),
        StructField('firstyear',   IntegerType(), True),
        StructField('lastyear',    IntegerType(), True)
    ])
    invframe = spark.createDataFrame(invsplitlines, invschema)
    invframe.createOrReplaceTempView("ghcndinventory")
    invframe.write.mode('overwrite').parquet(
        GHCNDPATH + "ghcndinventory.parquet")
    invframe.cache()
    print("Imported GhcndInventory")
    return invframe


def import_ghcnd_all(scon, spark):
    """
    Import all files from GHCND.

    Parameters
    ----------
    scon : SparkContext
        The context.
    spark : SparkSession
        The SQL session.

    Returns
    -------
    None.

    """
    localfilepath = "file://" + GHCNDHOMEPATH
    import_ghcnd_countries(scon, spark, localfilepath)
    import_ghcnd_stations(scon, spark, localfilepath)
    import_ghcnd_inventory(scon, spark, localfilepath)
    # import_all_data(scon, spark, GHCNDHOMEPATH)
    import_data_rdd_parallel(scon, spark, localfilepath)


def read_ghcnd_from_parquet(spark):
    """
    Read all data from the parquet files into Dataframes.

    Create temporary views from the parquet files.

    Parameters
    ----------
    spark : SparkSession
        The SQL Session.

    Returns
    -------
    None.

    """
    dfcountries = spark.read.parquet(GHCNDPATH + "ghcndcountries")
    dfcountries.createOrReplaceTempView("ghcndcountries")
    dfcountries.cache()

    dfstations = spark.read.parquet(GHCNDPATH + "ghcndstations")
    dfstations.createOrReplaceTempView("ghcndstations")
    dfstations.cache()

    dfinventory = spark.read.parquet(GHCNDPATH + "ghcndinventory")
    dfinventory.createOrReplaceTempView("ghcndinventory")
    dfinventory.cache()

    dfdata = spark.read.parquet(GHCNDPATH + "ghcnddata")
    dfdata.createOrReplaceTempView("ghcnddata")
    dfdata.cache()


def delete_all_parquet_ghcnd():
    """
    Delete all parquet files that were imported from GHCND.

    Returns
    -------
    None.

    """
    delete_from_hdfs(GHCNDPATH + "ghcndstations.parquet")
    delete_from_hdfs(GHCNDPATH + "ghcndcountries.parquet")
    delete_from_hdfs(GHCNDPATH + "ghcndinventory.parquet")
    delete_from_hdfs(GHCNDPATH + "ghcnddata.parquet")


def delete_from_hdfs(path):
    """
    Delete the file in path from HDFS.

    Parameters
    ----------
    path : string
        Path of the file in HDFS.

    Returns
    -------
    None.

    """
    call("hdfs dfs -rm -R " + path,
         shell=True)