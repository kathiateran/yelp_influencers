from pyspark.sql import SparkSession


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("UsefulUsers").getOrCreate()

path = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Get the raw data CONVERTS JSON, used to be 'lines'
users_dataFrame = spark.read.json(path)

userReview_Count = users_dataFrame.select("user_id", "review_count", "useful", "fans", "friends").\
    orderBy("review_count", "useful", "fans", "friends", ascending=False)

friendCount = users_dataFrame.select("friends").count().orderBy("count", ascending=False)

friends_join = userReview_Count.join(friendCount, on=['friends'], how=' leftOuterJoin').\
                               orderBy("friends", ascending=False)

friends_join.show()

#userFriends.take(30)
# Print the results
print("\n")

# Stop the session
spark.stop()