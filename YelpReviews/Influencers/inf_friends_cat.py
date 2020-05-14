from pyspark.sql import SparkSession

path1 = "/Volumes/KATHIA/A.\ SPARK/final_restaurants_all.json"
path2 = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_reviews_all.json"
path3 = "/Volumes/KATHIA/A.\ SPARK/final_restaurant_users_all.json"

# Create a SparkSession
spark = SparkSession.builder.appName("Popular_Reviewer").getOrCreate()

# Json raw data
business_dataFrame = spark.read.json(path1)
reviews_dataFrame = spark.read.json(path2)
users_dataFrame = spark.read.json(path3)

top_business = business_dataFrame.select("business_id", "name", "categories").orderBy("categories", ascending=False)
top_reviews = reviews_dataFrame.select("business_id", "user_id").orderBy("user_id", ascending=False)
top_reviewer = users_dataFrame.groupBy("friends").count().orderBy("count", ascending=False)
users_friends = users_dataFrame.select("user_id", "name", "friends").orderBy("friends", ascending=False)

inf_friendliest = top_reviewer.join(users_friends, on=['friends'], how='inner').orderBy("count", ascending=False).\
    where(top_reviewer.friends != 'None').drop(top_reviewer.friends)

top_reviewers = top_business.join(top_reviews, on=['business_id'], how='inner').orderBy("categories", ascending=False).\
                     join(inf_friendliest, on=['user_id'], how='inner').orderBy("count", ascending=False)

top_reviewers.show(30, False) #False shows full column content

# Print the results
print("\n")

# Stop the session
spark.stop()



