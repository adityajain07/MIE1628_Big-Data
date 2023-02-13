from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.tuning import CrossValidatorModel


# start spark session
spark = SparkSession.builder.appName('rec-eval').getOrCreate()

# load trained model
model_path = 'rec-model-v01/'
model = CrossValidatorModel.read().load(model_path)

# print(model.bestModel._java_obj.parent().getMaxIter())

print(model.bestModel._java_obj.parent().getRank())
print(model.bestModel.params)



