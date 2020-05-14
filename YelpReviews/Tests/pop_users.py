from pyspark.sql import SparkSession

# THIS just looks up the movie names
path =  "/Volumes/KATHIA/A.\ SPARK/final_restaurant_reviews_all.json"

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("Popular_Reviewer").getOrCreate()

# Get the raw data CONVERTS JSON, used to be 'lines'
reviews_dataFrame = spark.read.json(path)

# Some SQL-style magic to sort all movies by popularity in one line!
top_reviews = reviews_dataFrame.select("business_id", "user_id").orderBy("user_id", ascending=False)

top_reviews.show()

# Print the results
print("\n")

# Stop the session
spark.stop()