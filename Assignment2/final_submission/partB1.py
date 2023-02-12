from pyspark.sql import SparkSession
from pyspark.sql import Row

# start spark session
spark = SparkSession.builder.appName('rec').getOrCreate()

# read csv file
rdd_orig   = spark.read.option("header", True).csv('movies.csv').rdd
print(f'The total number of ratings given are {len(rdd_orig.collect())}')

# calculate number of movies
rdd_movies = rdd_orig.map(lambda row: (row[0], 1)) \
                .reduceByKey(lambda a, b: a + b)
print(f'The total number of movies are {len(rdd_movies.collect())}')

# calculate number of unique users
rdd_users = rdd_orig.map(lambda row: (row[2], 1)) \
                .reduceByKey(lambda a, b: a + b)
print(f'The total number of unique users are {len(rdd_users.collect())}')

# calculate the range of ratings
rdd_ratings = rdd_orig.map(lambda row: (row[1], 1)) \
                .reduceByKey(lambda a, b: a + b)
print(f'The range of movies ratings is {rdd_ratings.collect()}')

# calculate top 20 movies with highest average rating
movies_count = {}
rdd_movies_count = rdd_orig.map(lambda row: (row[0],  1)) \
                           .reduceByKey(lambda a, b: a + b) 
for item in rdd_movies_count.collect():
    movies_count[item[0]] = item[1]

movies_avg_rating = []
rdd_movies_top_ratings = rdd_orig.map(lambda row: (row[0],  int(row[1]))) \
                                 .reduceByKey(lambda a, b: a + b) 
for item in rdd_movies_top_ratings.collect():
    movies_avg_rating.append((item[0], round(item[1]/movies_count[item[0]], 2)))

movies_avg_rating.sort(key=lambda x: x[1], reverse=True)
print(f'Top 20 movies with the highest average ratings are {movies_avg_rating[:20]}')

# calculate top 15 users that gave most number of highest (5) ratings
rdd_users_top_ratings = rdd_orig.filter(lambda row: row[1]=='5') \
                                .map(lambda row: (row[2], 1)) \
                                .reduceByKey(lambda a, b: a + b) \
                                .sortBy(lambda x: x[1], False)                    
print(f'Top 15 users who provided  maximum highest (5) ratings are {rdd_users_top_ratings.collect()[:15]}')



