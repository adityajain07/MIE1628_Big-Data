from pyspark.sql import SparkSession
import re

# start spark session
spark = SparkSession.builder.getOrCreate()

# read text file
rdd   = spark.sparkContext.textFile('shakespeare-1.txt')

# perform transformation
rdd   = rdd.flatMap(lambda x: x.split()) \
           .flatMap(lambda x: re.split(',|!|\.|-|\?|\'|\"|:|;|\[|\]', x)) \
           .map(lambda word: (word, 1)) \
           .reduceByKey(lambda a, b: a + b) \
           .sortBy(lambda x: x[1])

print(f'The 10 words with least count are {rdd.collect()[:10]}')
print(f'The 10 words with max count are {rdd.collect()[-2:-13:-1]}')


