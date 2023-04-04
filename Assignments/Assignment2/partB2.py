from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.evaluation import RegressionEvaluator

# start spark session
spark = SparkSession.builder.appName('rec').getOrCreate()

# read csv file
rdd_orig   = spark.read.option("header", True).csv('movies.csv').rdd
ratingsRDD = rdd_orig.map(lambda p: Row(userId=int(p[2]), movieId=int(p[0]),
                                     rating=int(p[1])))

# create training and test data
train_split      = 0.8
test_split       = 0.2
ratings          = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([train_split, test_split])

# model training
als   = ALS(maxIter=10, regParam=1, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(training)

# evaluate on the test test
predictions = model.transform(test)
evaluator   = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f'Root-mean-square error for ({train_split*100}, {test_split*100}) is {round(rmse, 2)}')


