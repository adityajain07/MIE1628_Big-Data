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

# chose the users required for prediction
users = ratings.select(als.getUserCol()).distinct()
users = users.filter((col("userId") == 10) | (col("userId") == 14))

movie_rec = model.bestModel.recommendForUserSubset(users, 15)

print(movie_rec.show())


