from pyspark.sql import SparkSession

path1 = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession
spark = SparkSession.builder.appName("Friendliest_Reviewer").getOrCreate()

# Json raw data
users_dataFrame = spark.read.json(path1)

top_reviewer = users_dataFrame.groupBy("friends").count().orderBy("count", ascending=False)
users_friends = users_dataFrame.select("user_id", "name", "friends").orderBy("friends", ascending=False)


inf_friendliest = top_reviewer.join(users_friends, on=['friends'], how='inner').orderBy("count", ascending=False).\
    where(top_reviewer.friends != 'None').drop(top_reviewer.friends)


inf_friendliest.show(2, False)


# Print the results
print("\n")

# Stop the session
spark.stop()
