from pyspark.sql import SparkSession
from pyspark.sql import functions as F
#import pandas as pd


# THIS just looks up the movie names
path = "/Volumes/KATHIA/A.\ SPARK/YelpReviews/final_restaurant_users_all.json"

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("PopularUsers").getOrCreate()
sc = spark.sparkContext

# Get the raw data CONVERTS JSON, used to be 'lines'
users_dataFrame = spark.read.json(path)

# Some SQL-style magic to sort all movies by popularity in one line!
userFriends = users_dataFrame.groupBy("friends").count().orderBy("count", ascending=False).cache()

topUserIDs = users_dataFrame.groupBy("user_id", "name", "useful", "elite", "fans", "friends").count().\
                                        orderBy("count", ascending=False).cache()

# Show the results at this point:
#|UserID|count|
#+-------+-----+
#|     50|  584|
#|    258|  509|
#|    100|  508|

userFriends.show()
topUserIDs.show()

together = userFriends.join(topUserIDs, on = ['user_id'], how='outer') #take out 'count' part

# Grab the top
top20 = topUserIDs.take(20)
top20friends = userFriends.take(20)



# Place the DataFrames side by side
#horizontal_stack = pd.concat([top20, top20friends], axis=1)

#horizontal_stack.show()

# Print the results
print("\n")

# Stop the session
spark.stop()
