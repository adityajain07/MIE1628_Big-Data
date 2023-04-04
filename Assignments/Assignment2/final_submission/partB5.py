from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql import Row
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col


# start spark session
spark = SparkSession.builder.appName('rec-eval').getOrCreate()

# load data
rdd_orig   = spark.read.option("header", True).csv('movies.csv').rdd
ratingsRDD = rdd_orig.map(lambda p: Row(userId=int(p[2]), movieId=int(p[0]),
                                     rating=int(p[1])))
ratings    = spark.createDataFrame(ratingsRDD)

# load trained model
als        = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
model_path = 'rec-model-v01/'
model      = CrossValidatorModel.read().load(model_path)

# store data of original ratings by select users
user1             = 10
user2             = 14
user1_ratings     = []
user2_ratings     = []
rdd_user1_ratings = ratingsRDD.filter(lambda row: row[0]==user1)
rdd_user2_ratings = ratingsRDD.filter(lambda row: row[0]==user2)

for row in rdd_user1_ratings.collect():
    user1_ratings.append(row[1])
for row in rdd_user2_ratings.collect():
    user2_ratings.append(row[1])

# make top n predictions for the select users
top_n_pred   = 15
total_movies = 100
users_all    = ratings.select(als.getUserCol()).distinct()
user1_id     = users_all.filter((col("userId") == user1))
user2_id     = users_all.filter((col("userId") == user2))
user1_pred   = []
user2_pred   = []

movie_recomm_user1 = model.bestModel.recommendForUserSubset(user1_id, total_movies).rdd
movie_recomm_user1 = movie_recomm_user1.collect()[0][1]
movie_recomm_user2 = model.bestModel.recommendForUserSubset(user2_id, total_movies).rdd
movie_recomm_user2 = movie_recomm_user2.collect()[0][1] 

count = 0
for item in movie_recomm_user1:
    if item[0] not in user1_ratings:
        user1_pred.append(item[0])
        count +=1
    if count == top_n_pred:
        break
count = 0
for item in movie_recomm_user2:
    if item[0] not in user2_ratings:
        user2_pred.append(item[0])
        count +=1
    if count == top_n_pred:
        break

print(f'The top {top_n_pred} movie recommendations for user id {user1} is {user1_pred}')
print(f'The top {top_n_pred} movie recommendations for user id {user2} is {user2_pred}')