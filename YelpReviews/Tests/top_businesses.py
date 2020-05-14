from pyspark.sql import SparkSession

# THIS just looks up the movie names
path = "/Volumes/KATHIA/A.\ SPARK/final_restaurants_all.json"

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("Popular_Businesses").getOrCreate()

# Get the raw data CONVERTS JSON, used to be 'lines'
business_dataFrame = spark.read.json(path)

# Some SQL-style magic to sort all movies by popularity in one line!
top_business = business_dataFrame.select("business_id", "name", "city", "review_count").orderBy("review_count", ascending=False)

top_business.show(10, False)

# Print the results
print("\n")

# Stop the session
spark.stop()