from pyspark.sql import SparkSession

path = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession
spark = SparkSession.builder.appName("PopularUsers").getOrCreate()

#JSON raw data
users_dataFrame = spark.read.json(path)

userReview_Count = users_dataFrame.select("user_id", "name", "review_count", "average_stars").\
                       orderBy("review_count", "average_stars", ascending=False)

userReview_Count.show(10, False)

# Print the results
print("\n")

# Stop the session
spark.stop()