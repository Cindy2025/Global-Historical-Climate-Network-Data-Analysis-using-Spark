
# Analysis Q3
# Analysis Q3(a)

# get the default blocksize 
command hdfs getconf -confKey "dfs.blocksize"   #134217728B(128MB)
# file size for 2010 and 2020
hdfs dfs -du /data/ghcnd/daily/2010.csv.gz #232080599B(221.3MB)
hdfs dfs -du /data/ghcnd/daily/2020.csv.gz #31626590B(30.16MB)

# get the individual block sizes for the year 2010
hdfs fsck /data/ghcnd/daily/2010.csv.gz -files -blocks #116040299B(110.6MB) on average
#(b)

#Load and count the number of observations in daily for each of the years 2015 and 2020.

#Load the year 2015 and 2020 

daily_2015 = (spark.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(SchemaDaily)
    .option("dateFormat","yyyyMMdd")
    .load("hdfs:///data/ghcnd/daily/2015.csv.gz")
 )
daily_2015.cache()
#daily_2015.show()
daily_2015.count()#34899014


daily_2020 = (spark.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(SchemaDaily)
    .option("dateFormat","yyyyMMdd")
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")
 )
daily_2020.cache()
#daily_2020.show()
daily_2020.count()#5215365

#(c)
#Load and count the number of observations in daily from 2015 to 2020.

daily_2015_2020 = (spark.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(SchemaDaily)
    .option("dateFormat","yyyyMMdd")
    .load("hdfs:///data/ghcnd/daily/20{15,16,17,18,19,20}.csv.gz")
 )
daily_2015_2020.count()#178918901