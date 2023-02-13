from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
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

# instantiating model 
als   = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")

# # hyperparameter search
parameters=ParamGridBuilder() \
                .addGrid(als.maxIter, [5]) \
                .addGrid(als.rank, [3]) \
                .addGrid(als.regParam, [1]) \
                .build()
                

# # evaluation metric
evaluator   = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")

# build train, validation split
trainvs = TrainValidationSplit(
                estimator=als,
                estimatorParamMaps=parameters,
                evaluator=evaluator
                )
# cv = CrossValidator(
#         estimator=als,
#         estimatorParamMaps=parameters, 
#         evaluator=evaluator,
#         parallelism=2)

# fit model on to training data and evaluate using validation data
model = trainvs.fit(training)
# model = cv.fit(training)
best_model = model.bestModel
model_path = 'rec-model-v01/'
print("**Best Model**")
# Print "Rank"
print("  Rank:", best_model._java_obj.parent().getRank())
# Print "MaxIter"
print("  MaxIter:", best_model._java_obj.parent().getMaxIter())
# Print "RegParam"
print("  RegParam:", best_model._java_obj.parent().getRegParam())
model.write().overwrite().save(model_path)

# print('The validation metrics are: ', model.validationMetrics)

# evaluate on the test test
predictions = model.transform(test)
rmse = evaluator.evaluate(predictions)

print(f'Root mean squared error (RMSE) is {round(rmse, 2)}')


