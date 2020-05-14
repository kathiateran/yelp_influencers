from pyspark.sql import SparkSession

path = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession
spark = SparkSession.builder.appName("UsefulUsers").getOrCreate()

#JSON raw data
users_dataFrame = spark.read.json(path)

yelping_since = users_dataFrame.select("user_id", "name", "yelping_since").orderBy("yelping_since", ascending=True)

yelping_since.show(10, False)

# Print the results
print("\n")

# Stop the session
spark.stop()