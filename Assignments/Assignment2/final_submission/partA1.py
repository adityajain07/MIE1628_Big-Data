from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

rdd      = spark.sparkContext.textFile('integer.txt')
rdd      = rdd.flatMap(lambda line: line.split())
rdd_even = rdd.filter(lambda x: int(x) % 2 == 0)
rdd_odd  = rdd.filter(lambda x: int(x) % 2 == 1)

print(f'Count of even numbers is {len(rdd_even.collect())}')
print(f'Count of odd numbers is {len(rdd_odd.collect())}')


