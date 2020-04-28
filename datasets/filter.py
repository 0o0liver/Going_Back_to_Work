import sys
from pyspark.sql import SparkSession
from geopy.geocoders import Nominatim
import pyspark.sql.functions as F
from pyspark.sql.types import *

def get_address(lat, lng):
	geolocator = Nominatim(user_agent = "going_back_to_work")
	geo_str = str(lat) + ", " + str(lng)
	location = geolocator.reverse(geo_str)
	return location.address

def get_group_tag(datetime):
	lst = str(datetime).strip().split(" ")
	timeLst = lst[-1].strip().split(":")
	date_tag = lst[0]
	range_tag = ""
	if int(timeLst[1]) <= 10:
		range_tag = timeLst[0]+":00"+"~"+timeLst[0]+":10"
	elif int(timeLst[1]) <= 20:
		range_tag = timeLst[0]+":11"+"~"+timeLst[0]+":20"
	elif int(timeLst[1]) <= 30:
		range_tag = timeLst[0]+":21"+"~"+timeLst[0]+":30"
	elif int(timeLst[1]) <= 40:
		range_tag = timeLst[0]+":31"+"~"+timeLst[0]+":40"
	elif int(timeLst[1]) <= 50:
		range_tag = timeLst[0]+":41"+"~"+timeLst[0]+":50"
	else:
		range_tag = timeLst[0]+":51"+"~"+timeLst[0]+":59"
	return date_tag + " " + range_tag

if __name__ == "__main__":
	spark = SparkSession.builder.appName("filter").getOrCreate()

	trips = spark.read.format("csv").options(header="true").load(sys.argv[1])

	query_addr = sys.argv[2] + ", " + sys.argv[3]
	
	geo_to_loc = F.udf(get_address, StringType())

	datetime_to_tag = F.udf(get_group_tag, StringType())

	new_trips = trips.withColumn("End_Addr", geo_to_loc("End_Lat", "End_Lon")).withColumn("Datetime_tag", datetime_to_tag("Trip_Dropoff_DateTime"))
	
	result = new_trips.filter(new_trips.End_Addr.contains(query_addr)).groupby("Datetime_tag").count().orderBy("Datetime_tag")

	result.toPandas().to_csv("datetime_count.csv", header=True, index=False)
