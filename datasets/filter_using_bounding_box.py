import sys
from pyspark.sql import SparkSession
from geopy.geocoders import Nominatim
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Get geolocation of query address
query_addr = sys.argv[2]
geolocator = Nominatim(user_agent = "query_addr")
target_location = geolocator.geocode(query_addr)
target_lat = target_location.latitude
target_lng = target_location.longitude

# Utility functions
def get_address(lat, lng):
	'''
	output string address of (lat, lng)
	'''
	geo_str = str(lat) + ", " + str(lng)
	location = geolocator.reverse(geo_str)
	return location.address

def get_bounding_box(lat, lng):
	'''
	output string bounding box of (lat, lng)
	'''
	try:
		geo_str = str(lat) + ", " + str(lng)
		location = geolocator.reverse(geo_str).raw
		return ",".join(location["boundingbox"])
	except KeyError:
		print(str(lat) + ", " + str(lng))
		return ""

def filter_loc(box):
	'''
	check if target location is in a bounding box
	'''
	boxLst = list(map(float, box.split(",")))
	if min(boxLst[0], boxLst[1]) <= target_lat <= max(boxLst[0], boxLst[1]) and min(boxLst[2], boxLst[3]) <= target_lng <= max(boxLst[2], boxLst[3]):
		return True
	return False

def get_group_tag(datetime):
	'''
	output a tag for dataframe to group on. 
	format: yyyy-mm-dd hh-mm~hh-mm.
	time are grouped to 10 min interval
	'''
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

# Execution code
spark = SparkSession.builder.appName("going_back_work").getOrCreate()

trips = spark.read.format("csv").options(header="true").load(sys.argv[1])

geo_to_box = F.udf(get_bounding_box, StringType())

datetime_to_tag = F.udf(get_group_tag, StringType())

filter_func = F.udf(filter_loc, BooleanType())

new_trips = trips.withColumn("End_Box", geo_to_box("End_Lat", "End_Lon")).withColumn("Datetime_tag", datetime_to_tag("Trip_Dropoff_DateTime"))

#new_trips.write.format("csv").save("taxi_with_extra_column")

#new_trips.toPandas().to_csv("trips.csv", header=True, index=False)

#new_trips.show(truncate=100)

result = new_trips.filter(filter_func(new_trips["End_Box"])).groupby("Datetime_tag").count().orderBy("Datetime_tag")

#result.show(truncate=100)

#result.write.format("csv").save("datetime_count")

#result.toPandas().to_csv("datetime_count.csv", header=True, index=False)
