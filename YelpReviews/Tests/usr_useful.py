#import pyspark
from pyspark.sql import SparkSession


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("UsefulUsers").getOrCreate()
#sc = spark.sparkContext

path = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Get the raw data CONVERTS JSON, used to be 'lines'
users_dataFrame = spark.read.json(path)

# Some SQL-style magic to sort all movies by popularity in one line!
#userFunny = users_dataFrame.select("user_id", "funny").orderBy("funny", ascending=False).cache()
#userCool = users_dataFrame.select("user_id", "cool").orderBy("cool", ascending=False)
userReview_Count = users_dataFrame.select("user_id", "review_count").orderBy("review_count", ascending=False)

userFriends = users_dataFrame.select("user_id", "friends").orderBy("friends", ascending=False)
friendCount = users_dataFrame.select("friends").count().orderBy("count", ascending=False)

friends_join = friendCount.join(userFriends, on=['friends'], how=' leftOuterJoin').\
                               orderBy("friends", ascending=False)

#third_two = firstTwo_join.join(userReview_Count, on=['user_id'], how='inner')

#friendCount.take(30)

friends_join.show()

#userFriends.take(30)
# Print the results
print("\n")

# Stop the session
spark.stop()