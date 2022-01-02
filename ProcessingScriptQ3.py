 

# Processing Q3
# (a) Extract the two character country code from each station code in stations and store the
# output as a new column using the withColumn command.
stations=stations1.withColumn('CountryCode', F.col('ID').substr(1,2))
stations.show()
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+------------+
#|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|Country_Code|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+------------+
#|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |          AC|
#|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |          AC|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+------------+

#(b) LEFT JOIN stations with countries using your output from part (a).

stations_countries = (
    stations
    .withColumn("COUNTRY_CODE", F.trim(F.substring(F.col("ID"), 1, 2)))
    .join(
        countries1
            .select(
                F.col("CODE").alias("COUNTRY_CODE"),
                F.col("NAME").alias("COUNTRY_NAME")
            ),
        on = "COUNTRY_CODE",
        how = "left"
    )
)
stations.show(2)

#+------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-------------------+
#|COUNTRY_CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|       COUNTRY_NAME|
#+------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-------------------+
#|          AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |Antigua and Barbuda|
#|          AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |Antigua and Barbuda|
#+------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-------------------+

#(c)LEFT JOIN stations and states, allowing for the fact that state codes are only provided
#for stations in the US.

stations1_ID = stations1.withColumn("Country_Code", F.trim(F.substring(F.col("ID"), 1, 2)))

states1_ID = (states1.select(F.col("CODE").alias("Country_Code")))
stations1_states1= stations1_ID.join(states1_ID , on = "Country_Code",how = "left")
	
	
 
stations1_states1.show(2)

#+------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
#|Country_Code|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|
#+------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
#|          AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |
#|          AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |
#+------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
 

#(d) Based on inventory
#(1)what was the first and last year that each station was active and collected
#   any element at all?

station_first_last = (
    inventory1
    # Select only the columns that are needed
    .select(['ID','FIRSTYEAR','LASTYEAR'])
    # Group by ID and find the first and last year
    .groupBy('ID')
    .agg(
        F.min("FIRSTYEAR").alias("FIRST_YEAR"),
        F.max("LASTYEAR").alias("LAST_YEAR")
    )
    .orderBy('ID')
    
 )
 station_first_last.show(2)
#+-----------+----------+---------+
#|         ID|FIRST_YEAR|LAST_YEAR|
#+-----------+----------+---------+
#|ACW00011604|      1949|     1949|
#|ACW00011647|      1957|     1970|
#+-----------+----------+---------+


 

#(2)How many different elements has each station collected overall?

 
station_element_count = inventory1\
                            .select('ID', 'ELEMENT') \
                            .distinct() \
                            .groupby('ID') \
                            .agg(F.count('ELEMENT').alias('ELEMENT_COUNT')) \
                            .sort(F.desc('ELEMENT_COUNT'))
station_element_count.show(2, False)
#+-----------+-------------+
#|ID         |ELEMENT_COUNT|
#+-----------+-------------+
#|USW00014607|62           |
#|USW00013880|61           |
#+-----------+-------------+


#(3)count separately the number of core elements and the number of ”other” elements
#   that each station has collected overall.

core_element = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']

station_corelement_count = (inventory1
                                .select('ID', 'ELEMENT')
                                .distinct()
                                .withColumn('IS_CORE', F.col('ELEMENT').isin(core_element))
                                .groupby('ID', 'IS_CORE')
                                .agg(F.count('ELEMENT').alias('ELEMENT_COUNT'))
                                .groupby('ID')
                                .pivot('IS_CORE')
                                .sum('ELEMENT_COUNT')
                                .select('ID', F.col('true').alias('CORES_COUNT'), F.col('false').alias('OTHERS_COUNT'))
                                .sort(F.desc('CORES_COUNT'), F.desc('OTHERS_COUNT'))
                            )
station_corelement_count.show(2, False)
#+-----------+-----------+------------+
#|ID         |CORES_COUNT|OTHERS_COUNT|
#+-----------+-----------+------------+
#|USW00014607|5          |57          |
#|USW00013880|5          |56          |
#+-----------+-----------+------------+



 
#(4)How many stations collect all five core elements? 

station_5corelement = station_corelement_count.filter(F.col('CORES_COUNT') == 5).select(['ID','CORES_COUNT'])
station_5corelement.show(2)
#+-----------+-----------+
#|         ID|CORES_COUNT|
#+-----------+-----------+
#|USW00014607|          5|
#|USW00013880|          5|
#+-----------+-----------+



station_5corelement.count()#20266

#(5)How many only collected temperature?
#add a new column to inventory
station_temp_count = inventory1.withColumn('temp_elements',F.when(F.col('ELEMENT').isin(['TMAX', 'TMIN']) , 1).otherwise(3)) 
station_temp_count.show(2)
#+-----------+--------+---------+-------+---------+--------+-------------+
#|         ID|LATITUDE|LONGITUDE|ELEMENT|FIRSTYEAR|LASTYEAR|temp_elements|
#+-----------+--------+---------+-------+---------+--------+-------------+
#|ACW00011604|  17.116| -61.7833|   TMAX|     1949|    1949|            1|
#|ACW00011604|  17.116| -61.7833|   TMIN|     1949|    1949|            1|
#+-----------+--------+---------+-------+---------+--------+-------------+

station_temp_count.withColumn('temp_elements', F.col('temp_elements').cast(IntegerType()))
#to find the stations which only collected temperature(TMIN TMAX)
station_TMIN_TMAX = (
    station_temp_count
    .groupBy('ID')
    .agg({'temp_elements':'sum'})
    .orderBy('sum(temp_elements)', ascending=True)
    .select('ID',
        F.col('sum(temp_elements)').alias('num_temp'))
 )
 station_TMIN_TMAX.show(2)
 
#+-----------+--------+
#|         ID|num_temp|
#+-----------+--------+
#|BC000068234|       1|
#|USW00053846|       2|
#+-----------+--------+

 
station_temp_count2.filter(F.col('num_temp') <= 2).count()#301

# alternative way to do this problem
station_temp_count3 = (
     inventory1
     .groupby("ID")
     .agg(F.collect_set("ELEMENT"))
 )
station_temp_count3.show()

#（e）LEFT JOIN stations and your output from part (d).

 enriched_stations = (
    stations1_states1
    # left join to add first and last year of collecting
    .join(
        station_first_last,
        on='ID',
        how='left')
    # left join to the number of elements
    .join(
        station_element_count,
        on='ID',
        how='left')
     # left join to the number of core and other elements
    .join(
        station_corelement_count,
        on='ID',
        how='left')   
    # left join to if colleted five core elements and if only collected tempture
    .withColumn('FiveCore',F.when((F.col("CORES_COUNT") == 5), 'Yes').otherwise('No'))
    .join(
        station_TMIN_TMAX,
        on='ID',
        how='left') 
    .withColumn('OnlyTemp',F.when((F.col("num_temp") <= 2), 'Yes').otherwise('No'))
    .drop('num_temp')
 )

 enriched_stations.show()
#+-----------+-----------+--------------------+--------+---------+---------+---------+---------+--------------+--------+------------+------+--------------+-------------+-------------+--------+---------+--------+--------+
#|         ID|CountryCode|         CountryName|LATITUDE|LONGITUDE|ELEVATION|StateCode|StateName|   StationName|GSN FLAG|HCN/CRN FLAG|WMO ID|min(FIRSTYEAR)|max(LASTYEAR)|ELEMENT_COUNT|num_core|num_other|FiveCore|OnlyTemp|
#+-----------+-----------+--------------------+--------+---------+---------+---------+---------+--------------+--------+------------+------+--------------+-------------+-------------+--------+---------+--------+--------+
#|ACW00011647|         AC| Antigua and Barbuda| 17.1333| -61.7833|     19.2|         |     null|      ST JOHNS|        |            |      |          1957|         1970|            7|       5|        2|     Yes|      No|
#|AEM00041217|         AE|United Arab Emirates|  24.433|   54.651|     26.8|         |     null|ABU DHABI INTL|        |            | 41217|          1983|         2020|            4|       3|        1|      No|      No|
#+-----------+-----------+--------------------+--------+---------+---------+---------+---------+--------------+--------+------------+------+--------------+-------------+-------------+--------+---------+--------+--------+
stations_daily2.show()
a = Daily.select(F.col("ID"))
b = stations_enriched.select(F.col("ID"))
a.subtract(b).count()

#write the station_enriched data into hdfs output
#stations_enriched.write.mode('overwrite').csv('hdfs:///user/swu25/outputs/ghcnd/stations.csv')
stations_enriched.write.csv('hdfs:///user/yzh352/outputs/ghcnd/stations.csv.gz',mode='overwrite',header = True,compression="gzip") 

#(f) LEFT JOIN your 1000 rows subset of daily and your output from part (e). 
#(1)left join
stations_daily = (
    daily
    .join(
        stations_enriched,
        on='ID',
        how='left'
     )
    )
stations_daily.show()
#(2)left join using broadcast
stations_daily2 = (
    daily
    .join(
        F.broadcast(stations_enriched),
        on='ID',
        how='left'
     )
    )


#save the file to hdfs
stations_daily.write.csv('hdfs:///user/swu25/outputs/ghcnd/stations_daily1000.csv.gz',mode='overwrite',header = True,compression="gzip") 
stations_daily.select('ID','NAME').show()

#Are there any stations in your subset of daily that are not in stations at all?
#I tried 3 methods,all of the results are the same.

#method1
stations_daily.filter(F.col('LATITUDE') == "null").count()#0

#method2
# find the unique stations
station_in_daily = daily.select("ID").distinct()
daily.select("ID").distinct().count() # find out 318 unique stations
station_in_stations = stations_enriched.select("ID").distinct()
stations_enriched.select("ID").distinct().count()#115081 unique stations
diff = station_in_daily.subtract(station_in_stations)
diff.count()#0
 

 

 