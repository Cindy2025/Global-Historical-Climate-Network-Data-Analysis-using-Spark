

# Analysis Q4
# Analysis Q4(a)

Daily_all = (spark.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(SchemaDaily)
	.option("dateFormat","yyyyMMdd")    
    .load("hdfs:///data/ghcnd/daily/*.csv.gz")
    .repartition(100)
 )
Daily_all.count()#2928664523

 
(b)
#filter the data to that contains the five core elements.
daily_corelement = Daily_all.where(
(F.col('ELEMENT') == 'PRCP')
| (F.col("ELEMENT") == 'SNOW')
| (F.col("ELEMENT") == 'SNWD')
| (F.col("ELEMENT") == 'TMAX')
| (F.col("ELEMENT") == 'TMIN')
)
#count the observations according to the element
daily_corelement_obser = (
    daily_corelement
    .groupBy('ELEMENT')
    .agg(F.count(F.col('DATE')).alias('num_obser'))
    .orderBy('num_obser', ascending=False)
    .select('ELEMENT','num_obser')
    )
daily_corelement_obser.show()
#+-------+----------+
#|ELEMENT| num_obser|
#+-------+----------+
#|   PRCP|1021682210|
#|   TMAX| 436709350|
#|   TMIN| 435296249|
#|   SNOW| 332430532|
#|   SNWD| 283572167|
#+-------+----------+
#PRCP has most observations of them.

#load inventory
#  'inventory'
inventory_element = (
    inventory
    .groupBy('ELEMENT')
    .agg(F.count(F.col('ID')).alias('num_elem'))
    .orderBy('num_elem', ascending=False)
    .select('ELEMENT','num_elem')
    )           
inventory_element.show()#PRCP is still the most element.
 


#(c)How many observations of TMIN do not have a corresponding observation of TMAX

daily_corelement2 = daily_corelement.withColumn('TMIN',F.when((F.col("ELEMENT") == 'TMIN'), 1).otherwise(0))
#+-----------+--------+-------+-----+-----+-----+-----+--------+----+
#|         ID|    DATE|ELEMENT|VALUE|MFLAG|QFLAG|SFLAG|OBS_TIME|TMIN|
#+-----------+--------+-------+-----+-----+-----+-----+--------+----+
#|CA008504175|20111125|   SNWD| 70.0| null| null|    C|    null|   0|
#|ASN00098017|20111125|   TMIN|109.0| null| null|    a|    null|   1|
#+-----------+--------+-------+-----+-----+-----+-----+--------+----+

daily_corelement3 = daily_corelement2.withColumn('TMAX',F.when((F.col("ELEMENT") == 'TMAX'), 1).otherwise(0))
daily_corelement4 = (daily_corelement3
.select('ID', 'DATE', 'TMIN', 'TMAX')
.groupby('ID', 'DATE')
.agg(F.sum(F.col("TMIN")).alias('num_TMIN'),F.sum(F.col("TMAX")).alias('num_TMAX'))
.select('ID', 'DATE', 'num_TMIN', 'num_TMAX')
)
#+-----------+--------+--------+--------+
#|         ID|    DATE|num_TMIN|num_TMAX|
#+-----------+--------+--------+--------+
#|ACW00011604|19490121|       1|       1|
#|ACW00011604|19490314|       1|       1|
#+-----------+--------+--------+--------+

daily_TMIN_only = daily_corelement4.filter((F.col('num_TMIN') == 1) & (F.col('num_TMAX') == 0))
daily_TMIN_only.show()
daily_TMIN_only.count() #8428801  

#how many different stations contributed to these observations?
daily_TMIN_only.select("ID").distinct().count() #27526 


#(d)Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand
daily_NZ = Daily_all.where(F.col('ID').substr(1,2) == 'NZ')
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
#|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT FLAG|QUALITY FLAG|SOURCE FLAG|OBSERVATION TIME|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
#|NZ000936150|20200101|   TMAX|305.0|            null|        null|          S|            null|
#|NZ000936150|20200101|   TMIN|121.0|            null|        null|          S|            null|
#|NZ000936150|20200101|   PRCP|  0.0|            null|        null|          S|            null|
#|NZ000936150|20200101|   TAVG|195.0|               H|        null|          S|            null|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
###########################
daily_NZ_Temp = daily_NZ.where(F.col('ELEMENT').isin('TMIN','TMAX'))
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
#|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT FLAG|QUALITY FLAG|SOURCE FLAG|OBSERVATION TIME|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
#|NZ000936150|20200101|   TMAX|305.0|            null|        null|          S|            null|
#|NZ000936150|20200101|   TMIN|121.0|            null|        null|          S|            null|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+


#save the result to your output directory.
 
################ save
daily_NZ_Temp.repartition(1).write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/daily_NZ_Temp")
hadoop fs -copyToLocal /user/yzh352/outputs/ghcnd/msd1/part-00000-d23011a4-c7ea-42ed-ab59-8be0c96f8761-c000.csv
 
#how many observations?
daily_NZ_Temp.count()#458892
#How many years are covered by the observations?
NZ_Tem_Year = (daily_NZ_Temp
				.withColumn('Year',F.col('DATE').substr(1,4))
				.select('Year')
				.distinct()
				.count()
) #81

st = stations1.select("ID", "NAME")			
s_daily = daily_NZ_Temp.select("ID", "Date","ELEMENT", "VALUE")
m_s_d = s_daily.join(F.broadcast(st), on="ID", how="left")
m_s_d1 = m_s_d.select("Date","ELEMENT", "VALUE","NAME").orderBy("NAME", "DATE")
m_s_d1.repartition(1).write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/msd1")
################ average for TMIN & TMAX
m_s_d_avg =( m_s_d.select("Date","ELEMENT", "VALUE","NAME")
				.groupBy("DATE", "ELEMENT")
                .agg(F.avg("VALUE")).alias("Avg")
)
m_s_d_avg.repartition(1).write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/msavg")

# Use bash cou the number of file rows. 			
$ wc -l /user/abc123/outputs/msd1.csv

# Analysis Q4(e)
 
 
 
 
station2 = stations1.withColumn("Country_Code", F.trim(F.substring(F.col("ID"), 1, 2)))

station2.show(2)
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+---------+--------+------------+
#|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT FLAG|QUALITY FLAG|SOURCE FLAG|OBSERVATION TIME|LONGITUDE|LATITUDE|Country_Code|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+---------+--------+------------+
#|US1FLSL0019|20200101|   SNOW|  0.0|            null|        null|          N|            null| -80.3111| 27.3237|          US|
#|US1NVNY0012|20200101|   PRCP|  0.0|            null|        null|          N|            null|-116.0355| 36.2127|          US|
#+-----------+--------+-------+-----+----------------+------------+-----------+----------------+---------+--------+------------+
station3 =station2.select("LONGITUDE", "LATITUDE", "COUNTRY_CODE")
station3.show(2, False)
#+---------+--------+------------+
#|LONGITUDE|LATITUDE|COUNTRY_CODE|
#+---------+--------+------------+
#|-80.3111 |27.3237 |US          |
#|-116.0355|36.2127 |US          |
#+---------+--------+------------+


daily_PRCP_Year = (
    Daily_all
    .where(F.col("ELEMENT") == "PRCP")
    .groupBy(
        F.trim(F.substring(F.col("ID"), 1, 2)),
        F.trim(F.substring(F.col("DATE"), 1, 4))
    ).
    agg(F.round(F.mean("VALUE"), 2).alias("AVG_PRCP"))
    .withColumnRenamed("trim(substring(ID, 1, 2))" ,"COUNTRY_CODE")
    .withColumnRenamed("trim(substring(DATE, 1, 4))", "YEAR")
	
    )
#+------------+----+--------+
#|COUNTRY_CODE|YEAR|AVG_PRCP|
#+------------+----+--------+
#|          US|2020|   13.74|
#|          MX|2020|     0.0|
#+------------+----+--------+

daily_PRCP_Country = (
        countries1
        .select(
            F.col('CODE').alias('COUNTRY_CODE'), F.col('NAME').alias('COUNTRY_NAME')
        )
)

daily_PRCP_Year_Country = daily_PRCP_Year.join(
        broadcast(daily_PRCP_Country),
	    on = "COUNTRY_CODE", 
        how = "left"
		)
daily_PRCP_Year_Country.show(2,False)
#+------------+----+--------+-------------+
#|COUNTRY_CODE|YEAR|AVG_PRCP|COUNTRY_NAME |
#+------------+----+--------+-------------+
#|US          |2020|13.74   |United States|
#|MX          |2020|0.0     |Mexico       |
#+------------+----+--------+-------------+
L_L = daily_PRCP_Year_Country.join(F.broadcast(station3), on="COUNTRY_CODE", how="left")
L_L.show(2, False)
#+------------+----+--------+-------------+---------+--------+
#|COUNTRY_CODE|YEAR|AVG_PRCP|COUNTRY_NAME |LONGITUDE|LATITUDE|
#+------------+----+--------+-------------+---------+--------+
#|US          |2020|13.74   |United States|-149.3986|68.6483 |
#|US          |2020|13.74   |United States|-150.8747|63.4519 |
#+------------+----+--------+-------------+---------+--------+
L_L.repartition(1).write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/LL")

 


daily_PRCP_Year_Country.repartition(1).write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/daily_PRCP_Year_Country")
hadoop fs -copyToLocal /user/yzh352/outputs/ghcnd/daily_PRCP_Year_Country/part-00000-d23011a4-c7ea-42ed-ab59-8be0c96f8761-c000.csv


daily_PRCP_Year_Country.orderBy(F.col("AVG_PRCP"), ascending=False).show()

daily_PRCP_Year_Country.filter(F.col("COUNTRY_CODE") == "EK").orderBy(F.col("YEAR"), ascending=False).show()

daily_PRCP_Year_Country.filter(F.col("COUNTRY_CODE") == "EK").filter(F.col("AVG_PRCP") != 4361.0).agg(F.round(F.mean(F.col("AVG_PRCP")), 2)).show()

 
 
