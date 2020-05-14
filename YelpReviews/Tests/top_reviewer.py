from pyspark.sql import SparkSession

# THIS just looks up the movie names
path1 = "/Volumes/KATHIA/A.\ SPARK/final_restaurants_all.json"
path2 = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_reviews_all.json"
path3 = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("Popular_Reviewer").getOrCreate()

# Get the raw data CONVERTS JSON, used to be 'lines'
business_dataFrame = spark.read.json(path1)
reviews_dataFrame = spark.read.json(path2)
users_dataFrame = spark.read.json(path3)

# Some SQL-style magic to sort all movies by popularity in one line!
top_business = business_dataFrame.select("business_id", "name", "city").orderBy("city", ascending=False)
top_reviews = reviews_dataFrame.select("business_id", "user_id").orderBy("user_id", ascending=False)
top_reviewer = users_dataFrame.select("user_id", "name", "review_count").orderBy("review_count", ascending=False)

top_reviewers = top_business.join(top_reviews, on=['business_id'], how='inner').orderBy("city", ascending=False).\
                     join(top_reviewer, on=['user_id'], how='inner').orderBy("review_count", ascending=False)

top_reviewers.show(10, False)

# Print the results
print("\n")

# Stop the session
spark.stop()