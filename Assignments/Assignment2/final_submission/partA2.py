from pyspark.sql import SparkSession

# start spark session
spark = SparkSession.builder.getOrCreate()

# read text file
rdd      = spark.sparkContext.textFile('salary.txt')

# perform transformation
rdd      = rdd.flatMap(lambda line: line.split("\n")) \
              .map(lambda x: (x.split(" ")[0], int(x.split(" ")[1]))) \
              .reduceByKey(lambda a, b: a + b)

print(f'Salary sum per department is {rdd.collect()}')



