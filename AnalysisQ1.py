#
# Analysis Q1
# Analysis Q1(a)
#increase resources
start_pyspark_shell -e 4 -c 2 -w 4 -m 4 #increase resources
# How many stations are there in total? 

enriched_stations.agg(F.countDistinct("ID")).show()

# output: 115081

# How many stations have been active in 2020? 
enriched_stations.select(F.col('FIRST_YEAR')) 
enriched_stations.filter(F.col("LAST_YEAR") <= 2000).count()

# output: 43518

# How many stations are in each of the GCOS Surface Network (GSN) ? 

enriched_stations.filter(F.col("GSN FLAG") == "GSN").agg(F.countDistinct("ID")).show()

# output: 991

# How many stations are in each of the US Historical?

enriched_stations.filter(F.col("HCN/CRN FLAG") == "HCN").agg(F.countDistinct("ID")).show()

# output: 1218

# How many stations are in each of Climatology Network (HCN) ?

enriched_stations.filter(F.col("HCN/CRN FLAG") == "CRN").agg(F.countDistinct("ID")).show()

# output: 233

# How many stations are in each of the US Climate Reference Network (CRN)? 
# Are there any stations that are in more than one of these networks?

one_more = stations.where((F.col('GSN FLAG') != '') & (F.col('HCN/CRN FLAG') !='')).count()#14

# output: 14


# Analysis Q1(b)

enriched_stations_countrynum = (
    enriched_stations
    .groupBy("COUNTRY_CODE")
    .count()
    .withColumnRenamed("count", "STATION_BY_COUNTRY_NUM")
    .join(
        countries1
        .select(F.col("CODE").alias("COUNTRY_CODE"), F.col("NAME").alias("COUNTRY_NAME")),
        on = "COUNTRY_CODE",
        how = "left"
    )
)
# for states 
enriched_stations_statenum = (
    enriched_stations
    .groupBy("COUNTRY_CODE")
    .count()
    .withColumnRenamed("count", "STATION_BY_state_NUM")
    .join(
        states1
        .select(F.col("CODE").alias("COUNTRY_CODE"), F.col("NAME").alias("state_NAME")),
        on = "COUNTRY_CODE",
        how = "left"
    )
)
#+------------+----------------------+----------+
#|COUNTRY_CODE|STATION_BY_state_NUM  |state_NAME|
#+------------+----------------------+----------+
#|          AC|                     2|      null|
#|          AQ|                    20|      null|
#+------------+----------------------+----------+


enriched_stations_countrynum.agg(F.max(F.col("STATION_BY_COUNTRY_NUM"))).show()

enriched_stations_countrynum.orderBy(F.col("STATION_BY_COUNTRY_NUM"), ascending = False).show()

enriched_stations_countrynum.write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/enriched_stations_countrynumber.csv")
enriched_stations_statenum.write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/enriched_stations_countrynumber.csv")


# Analysis Q1(c)

# How many stations are there in the Northern Hemisphere only?
# Some of the countries in the database are territories of the United States as indicated by the name of the country.
# How many stations are there in total in the territories of the United States around the world?

print(enriched_stations.filter(F.col("LATITUDE") < 0).count())

# output: 23336

print(enriched_stations.where((F.col("COUNTRY_NAME").contains("United States")) | (F.col("COUNTRY_CODE") == "US")).count())

# output: 62183