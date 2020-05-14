from pyspark.sql import SparkSession

# THIS just looks up the movie names
path1 = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("Friendliest_Reviewer").getOrCreate()

# Get the raw data CONVERTS JSON, used to be 'lines'
users_dataFrame = spark.read.json(path1)

# Some SQL-style magic to sort all movies by popularity in one line!
top_reviewer = users_dataFrame.groupBy("friends").count().orderBy("count", ascending=False)
users_friends = users_dataFrame.select("user_id", "name", "friends").orderBy("friends", ascending=False)


inf_friendliest = top_reviewer.join(users_friends, on=['friends'], how='inner').orderBy("count", ascending=False).\
    where(top_reviewer.friends != 'None').drop(top_reviewer.friends)


#inf_friendliest.show(10, False)
#inf_friendliest.where(users_dataFrame.friends != 'None')

inf_friendliest.show(3, False)


# Print the results
print("\n")

# Stop the session
spark.stop()

#.orderBy("co
# unt", ascending=False)