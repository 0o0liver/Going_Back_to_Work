import sys
from pyspark.sql import SparkSession
from geopy.geocoders import Nominatim
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Get geolocation of query address
query_addr = sys.argv[2]
geolocator = Nominatim(user_agent = "going_bakc_to_work")
target_location = geolocator.geocode(query_addr)
target_box = list(map(float, target_location.raw["boundingbox"]))
start_lat = min(target_box[0], target_box[1])
end_lat = max(target_box[0], target_box[1])
start_lng = min(target_box[2], target_box[3])
end_lng = max(target_box[2], target_box[3])

# Utility functions
def filter_location(lat, lng):
	'''
	check if location in in target_box
	'''
	if start_lat <= float(lat) and float(lat) <= end_lat and start_lng <= float(lng) and float(lng) <= end_lng:
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

datetime_to_tag = F.udf(get_group_tag, StringType())

filter_function = F.udf(filter_location, BooleanType())

result_df = trips.withColumn("Datetime_tag", datetime_to_tag("Trip_Dropoff_DateTime")).filter(filter_function(trips["End_Lat"], trips["End_Lon"])).groupby("Datetime_tag").count().orderBy("Datetime_tag")

result_df.toPandas().to_csv("date_count.csv", header=True, index=False)
