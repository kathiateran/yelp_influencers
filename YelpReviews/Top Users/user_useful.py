from pyspark.sql import SparkSession

path = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession
spark = SparkSession.builder.appName("PopularUsers").getOrCreate()

#JSON raw data
users_dataFrame = spark.read.json(path)

user_useful = users_dataFrame.select("user_id", "name" , "useful").orderBy("useful", ascending=False)

user_useful.show(10, False)

# Print the results
print("\n")

# Stop the session
spark.stop()