from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# read text file
rdd      = spark.sparkContext.textFile('salary.txt')

# perform transformation
rdd      = rdd.flatMap(lambda line: line.split("\n")) 

print(rdd.collect())



