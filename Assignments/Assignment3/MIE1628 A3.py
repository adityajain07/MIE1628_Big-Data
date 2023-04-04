# Databricks notebook source
import urllib.request 
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import matplotlib.pyplot as plt
from pyspark.sql.functions import when
from pyspark.ml.classification import LinearSVC, GBTClassifier, MultilayerPerceptronClassifier
from pyspark.ml.feature import (StringIndexer, OneHotEncoder, VectorAssembler)
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A2

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/kddcup_data_10_percent.gz"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A3

# COMMAND ----------

# start spark session
spark = SparkSession.builder.getOrCreate()

# filename and path
filepath = 'dbfs:/FileStore/tables/kddcup_data_10_percent.gz'

# read text file
rdd   = spark.sparkContext.textFile(filepath)

# print top 10 values
print(f'The first 10 values of the data are: {rdd.take(10)}')
print(f'The type of data structure is {type(rdd)}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A4

# COMMAND ----------

rdd_processed   = rdd.flatMap(lambda x: x.split())
print(f'An example entry in the RDD is {rdd_processed.take(1)}')

example_row = rdd_processed.take(1)
example_row = example_row[0].split(",")
print(f'The total number of features are {len(example_row)}')
print(f'The total number of entries are {len(rdd_processed.collect())}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A5

# COMMAND ----------

rdd_six_columns = rdd_processed.map(lambda p: Row(
                                    duration      = int(p.split(",")[0]), 
                                    protocol_type = str(p.split(",")[1]),
                                    service       = str(p.split(",")[2]),
                                    flag          = str(p.split(",")[3]),
                                    src_bytes     = int(p.split(",")[4]),
                                    dst_bytes     = int(p.split(",")[5]),
                                    label         = str(p.split(",")[-1])
                                    ))

df = rdd_six_columns.toDF()
df.printSchema()
df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A6

# COMMAND ----------

# analysis based on protocol type
data_protocol_type = df.rdd.map(lambda x: x.protocol_type).collect()

dictionary = {}
for item in data_protocol_type:
    if item not in dictionary.keys():
        dictionary[item] = 1
    else:
        dictionary[item] += 1
        
dictionary = dict(sorted(dictionary.items(), key=lambda item: item[1]))
print(f'The total number of connections based on protocol_type are {dictionary}')

# plot
names  = list(dictionary.keys())
values = list(dictionary.values())

plt.bar(range(len(dictionary)), values, tick_label=names)
plt.xlabel('Protocol Type')
plt.ylabel('Count')
plt.show()

# COMMAND ----------

# analysis based on service type
data_service_type = df.rdd.map(lambda x: x.service).collect()

dictionary = {}
for item in data_service_type:
    if item not in dictionary.keys():
        dictionary[item] = 1
    else:
        dictionary[item] += 1
        
dictionary = dict(sorted(dictionary.items(), key=lambda item: item[1]))
print(f'The total number of connections based on service are {dictionary}')

# plot
names  = list(dictionary.keys())
values = list(dictionary.values())

plt.figure(figsize=(8,14))
plt.barh(names, values)
plt.xlabel('Count')
plt.ylabel('Service Type')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A7

# COMMAND ----------

# analysis based on label
data_label_type = df.rdd.map(lambda x: x.label).collect()

dictionary = {}
for item in data_label_type:
    if item not in dictionary.keys():
        dictionary[item] = 1
    else:
        dictionary[item] += 1
        
dictionary = dict(sorted(dictionary.items(), key=lambda item: item[1]))
print(f'The total number of connections based on label type are {dictionary}')

# plot
names  = list(dictionary.keys())
values = list(dictionary.values())

plt.figure(figsize=(6,8))
plt.barh(names, values)
plt.xlabel('Count')
plt.ylabel('Label Type')
plt.show()

# COMMAND ----------

# analysis based on flag
data_flag_type = df.rdd.map(lambda x: x.flag).collect()

dictionary = {}
for item in data_flag_type:
    if item not in dictionary.keys():
        dictionary[item] = 1
    else:
        dictionary[item] += 1
        
dictionary = dict(sorted(dictionary.items(), key=lambda item: item[1]))
print(f'The total number of connections based on flag type are {dictionary}')

# plot
names  = list(dictionary.keys())
values = list(dictionary.values())

plt.bar(range(len(dictionary)), values, tick_label=names)
plt.ylabel('Count')
plt.xlabel('Flag Type')
plt.show()

# COMMAND ----------

# analysis based on bytes transfer
data_src_bytes = df.rdd.map(lambda x: x.src_bytes).collect()
data_dst_bytes = df.rdd.map(lambda x: x.dst_bytes).collect()

avg_src_bytes = round(sum(data_src_bytes)/len(data_src_bytes), 2)
avg_dst_bytes = round(sum(data_dst_bytes)/len(data_dst_bytes), 2)

print(f'The average number of bytes transferred from source to destination are {avg_src_bytes}')
print(f'The average number of bytes transferred from destination to source are {avg_dst_bytes}')

# plot
plt.bar(range(2), [avg_src_bytes, avg_dst_bytes], tick_label=['src_bytes', 'dst_bytes'])
plt.ylabel('Average bytes transferred')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part A8

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data transformation

# COMMAND ----------

# club all attack types to one form
df_new = df.withColumn("label", when(df.label == "normal.","normal").otherwise("attack"))

# convert categorical data to numeric data
categorical_columns = ['protocol_type', 'service', 'label', 'flag']
indexers            = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df_new) for column in categorical_columns]
pipeline            = Pipeline(stages=indexers)
df_new              = pipeline.fit(df_new).transform(df_new)

# aggregate data into a feature vector
assembler = VectorAssembler(
    inputCols=["duration", "protocol_type_index", "service_index", "flag_index", "src_bytes", "dst_bytes"],
    outputCol="features")
df_ml_data  = assembler.transform(df_new)
df_ml_data.show(5)

train_split      = 0.8
test_split       = 0.2
(training, test) = df_ml_data.randomSplit([train_split, test_split])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Model 1: Linear SVM

# COMMAND ----------

# import and fit model
lsvc      = LinearSVC(labelCol="label_index", maxIter=20, regParam=0.1)
lsvcModel = lsvc.fit(training)

# print the coefficients and intercept for linear SVC
print("Coefficients: " + str(lsvcModel.coefficients))
print("Intercept: " + str(lsvcModel.intercept))

# evaluate on the test test
predictions = lsvcModel.transform(test)
evaluator   = BinaryClassificationEvaluator(labelCol="label_index")
accuracy    = evaluator.evaluate(predictions)
print(f'The classification accuracy of the Linear SVM model is {accuracy*100}')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Model 2: Multilayer Perceptron Classifier

# COMMAND ----------

# number of layers in the model
layers = [6, 8, 7, 2]

# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=100, labelCol="label_index", layers=layers, blockSize=128)

# train the model
mlp_model = trainer.fit(training)

# evaluate on the test test
predictions = mlp_model.transform(test)
evaluator   = BinaryClassificationEvaluator(labelCol="label_index")
accuracy    = evaluator.evaluate(predictions)
print(f'The classification accuracy of the multilayer perceptron is {accuracy*100}')

# COMMAND ----------


