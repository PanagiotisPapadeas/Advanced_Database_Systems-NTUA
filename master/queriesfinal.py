from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
import sys
import time
from datetime import datetime

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#start spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark session created")

#create dataframe for parquet data
parDF1=spark.read.parquet('hdfs://192.168.0.1:9000/user/user/data')
#parDF1=spark.read.parquet("data/")
parDF1 = parDF1.fitler((year(parDF1['tpep_pickup_datetime']) == "2022") & (month(parDF1['tpep_pickup_datetime']) < "7"))

#parDF1.count()
#parDF1.printSchema()
#parDF1.show(10)

#create rdd for parquet data
rdd1 = parDF1.rdd


schema2 = "LocationID INT, Borough STRING, Zone STRING, service_zone STRING"

#create dataframe for csv
DF2 = spark.read.csv('hdfs://192.168.0.1:9000/user/user/datacsv', schema = schema2)
#DF2 = spark.read.csv(path = "datacsv/taxi+_zone_lookup.csv", schema = schema2)
DF2.printSchema()

#rename column for inner join
DF2new = DF2.select(DF2['LocationID'].alias('DOLocationID'),"Borough","Zone","service_zone")

#Q1

start = time.time()

parDF1.join(DF2new, parDF1.DOLocationID ==  DF2new.DOLocationID,"inner").filter((month(parDF1['tpep_pickup_datetime']) == "3") & (DF2new['Zone'] == "Battery Park")).agg({"tip_amount":"max"}).show()

end = time.time()
time1 = end-start
print("TimeQ1:",time1)

#View for sql query

parDF1.createOrReplaceTempView("yellowtrip")
DF2new.createOrReplaceTempView("location")

start = time.time()
#sqlDF1 = spark.sql("select * from yellowtrip where tpep_pickup_datetime >= '2022-03-01 00:00:00' and tpep_pickup_datetime <= '2022-04-01 00:00:00' and DOLocationID = 12 and tip_amount in (select max(tip_amount) from yellowtrip where tpep_pickup_datetime >= '2022-03-01 00:00:00' and tpep_pickup_datetime <= '2022-04-01 00:00:00' and DOLocationID = 12)")
#sqlDF1.show()
sqlDF1 = spark.sql("select max(tip_amount) from yellowtrip INNER JOIN location ON yellowtrip.DOLocationID == location.DOLocationID where month(tpep_pickup_datetime) = 3 and Zone = 'Battery Park'")
sqlDF1.show()

end = time.time()
time1s = end-start
print("TimeQ1sql:",time1s)

#Q2

start = time.time()

parDF1.groupby(month("tpep_pickup_datetime").alias("month")).max("tolls_amount").show()

end = time.time()
time2 = end-start
print("TimeQ2:",time2)

#Q3

start = time.time()

#parDF1.filter(parDF1['PULocationID']!=parDF1['DOLocationID']).groupby(month("tpep_pickup_datetime").alias("month"),(dayofmonth("tpep_pickup_datetime")<16).alias("1st half of month")).mean('trip_distance','total_amount').show()
parDF1.filter(parDF1['PULocationID']!=parDF1['DOLocationID']).groupby((floor((dayofyear("tpep_pickup_datetime")-1)/15)+1).alias("nth 15 days of year")).mean('trip_distance','total_amount').show()

end = time.time()
time3 = end-start
print("TimeQ3:",time3)

#rdd api

start = time.time()

def convert(row):
    day_str = row.tpep_pickup_datetime
    dist = row.trip_distance
    amount = row.total_amount
    #dt = datetime.strptime(day_str, '%Y-%m-%d %H:%M:%S')
    dayofyear = int(day_str.strftime("%j"))
    fifteen = ((dayofyear-1)//15)+1
    return (fifteen, (dist,amount,1))

rddx = rdd1.filter(lambda x: x.PULocationID != x.DOLocationID).map(convert)

#for x in rddx.collect():
#    print(x)

rddfinal = rddx.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2])).mapValues(lambda x: (x[0]/x[2],x[1]/x[2]))

for x in rddfinal.collect():
   print(x)

end = time.time()
time3r = end-start
print("TimeQ3rdd:",time3r)


#Q4

start = time.time()

#create window of records partitioned by day and descending order based on passenger_count
windowdays = Window.partitionBy(dayofweek("tpep_pickup_datetime")).orderBy(col("passenger_count").desc())
df1days = parDF1.withColumn("row_num",row_number().over(windowdays))
df1days.filter(col("row_num") <= 3).select(hour("tpep_pickup_datetime").alias("rush hours"),dayofweek("tpep_pickup_datetime").alias("day")).show()

end = time.time()
time4 = end-start
print("TimeQ4:",time4)

#Q5

start = time.time()

#add column tip percentage
df1months = parDF1.withColumn("percent", col("tip_amount")/col("fare_amount"))

#create window artitioned by month and descending order based on tip percentage
windowmonths = Window.partitionBy(month("tpep_pickup_datetime")).orderBy(col("percent").desc())
df1months2 = df1months.withColumn("row_num",row_number().over(windowmonths))
df1months2.filter(col("row_num") <= 5).select(dayofmonth("tpep_pickup_datetime").alias("day"),month("tpep_pickup_datetime").alias("month")).show()

end = time.time()
time5 = end-start
print("TimeQ5:",time5)
