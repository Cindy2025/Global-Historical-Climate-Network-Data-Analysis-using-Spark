
# Processing 

# Determine how the data is structured in /data/ghcnd/

hdfs dfs -ls /data/ghcnd/
hdfs dfs -ls /data/ghcnd/daily | head
hdfs dfs -ls /data/ghcnd/daily | tail

# /data/ghcnd/
# ├─ countries
# ├─ daily
# │  ├─ 1763.csv.gz
# │  ├─ 1764.csv.gz
# │  ├─ ...
# │  └─ 2020.csv.gz
# ├─ inventory
# ├─ states
# └─ stations

# Determine how many files there are in daily

hdfs dfs -ls /data/ghcnd/daily | wc -l

# Determine the size of all of the data and the size of daily specifically

hdfs dfs -du -h /data/ghcnd/
hdfs dfs -du -h /data/ghcnd/daily

# Peek at the top of each data file to check the schema is as described

hdfs dfs -cat /data/ghcnd/countries | head
hdfs dfs -cat /data/ghcnd/inventory | head
hdfs dfs -cat /data/ghcnd/states | head
hdfs dfs -cat /data/ghcnd/stations | head

# Gunzip daily on the fly

hdfs dfs -cat /data/ghcnd/daily/2017.csv.gz | gunzip | head
