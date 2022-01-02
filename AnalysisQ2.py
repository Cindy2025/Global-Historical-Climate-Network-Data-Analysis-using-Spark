
# Analysis Q2
# Analysis Q2(a)

# This function used the ‘haversine’ formula to calculate the great-circle distance between two points.

def Haversine(lon1,lat1, lon2,lat2):
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
 
	
    # computer distance using Haversine formula
	
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371
	dis = c*r
    return dis

distance = F.udf(Haversine, DoubleType())
# Analysis Q2(b)

stationsNZ = (
    enriched_stations
        .where(F.col("COUNTRYCODE") == "NZ")\
        .select(F.col("STATIONNAME").alias("STATION_ORIG"),
                F.col("LATITUDE").alias("LATITUDE_ORIG"),
                F.col("LONGITUDE").alias("LONGITUDE_ORIG"))\
    .crossJoin(enriched_stations.where(F.col("COUNTRY_CODE") == "NZ")
        .select(F.col("STATIONNAME").alias("STATION_DEST"),
                F.col("LATITUDE").alias("LATITUDE_DEST"),
                F.col("LONGITUDE").alias("LONGITUDE_DEST")))\
    .filter(F.col("STATION_ORIG") != F.col("STATION_DEST"))
)

NZstationsDistance = (
    stationsNZ
    .withColumn("DISTANCE(km)",
			round(
            distance(
                stationsNZ['LONGITUDE_ORIG'],
                stationsNZ['LATITUDE_ORIG'],
                stationsNZ['LONGITUDE_DEST'],
                stationsNZ['LATITUDE_DEST']
				),2
             
        
    )
)
)

NZstationsDistance.orderBy(F.col("DISTANCE(km)"), ascending = False).show()

NZstationsDistance.write.format("csv").save("hdfs:///user/yzh352/outputs/ghcnd/NZstationsDistance.csv")