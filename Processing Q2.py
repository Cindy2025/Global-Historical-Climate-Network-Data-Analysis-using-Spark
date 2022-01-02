# DATA420-20S2 Assignment 1
# Processing Q2

# Imports

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Define schema for daily and load first 100 rows of daily (csv format)

schema_daily = StructType([
    StructField('ID', StringType()),
    StructField('DATE', StringType()),
    StructField('ELEMENT', StringType()),
    StructField('VALUE', IntegerType()),
    StructField('MEASUREMENT FLAG', StringType()),
    StructField('QUALITY FLAG', StringType()),
    StructField('SOURCE FLAG', StringType()),
    StructField('OBSERVATION TIME', StringType()),
])
daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)
daily.cache()
daily.show(10, False)

# Load each of countries, inventory, states, stations (fixed width text format)

countries_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")
)
countries_text_only.show(10, False)

countries = countries_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(StringType()),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(StringType())
)
countries.show(10, False)

countries.count()

# ...
