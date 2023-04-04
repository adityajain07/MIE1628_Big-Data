from pyspark.sql import SparkSession
import re

# start spark session
spark = SparkSession.builder.getOrCreate()

# read text file
rdd   = spark.sparkContext.textFile('shakespeare-1.txt')

# word list
world_list = ['Shakespeare', 'why', 'Lord', 'Library',
              'GUTENBERG', 'WILLIAM', 'COLLEGE', 'WORLD']

# perform transformation
rdd   = rdd.flatMap(lambda x: x.split()) \
           .flatMap(lambda x: re.split(',|!|\.|-|\?|\'|\"|:|;|\[|\]', x)) \
           .map(lambda word: (word, 1)) \
           .reduceByKey(lambda a, b: a + b) \
           .filter(lambda x : x[0] in world_list)

print(rdd.collect())


