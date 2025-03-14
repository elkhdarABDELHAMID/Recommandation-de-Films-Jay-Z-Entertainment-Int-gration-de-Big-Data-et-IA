from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ALS Model").getOrCreate()

ratings_df = spark.read.parquet("/data/processed_data.parquet")

als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
model = als.fit(ratings_df)

user_recs = model.recommendForAllUsers(10)
recs_exploded = user_recs.select("userId", F.explode("recommendations").alias("rec")) \
                         .select("userId", F.col("rec.movieId").alias("movieId"), F.col("rec.rating").alias("predicted_rating"))

es = Elasticsearch("http://elasticsearch:9200")
recs_df = recs_exploded.toPandas()
for index, row in recs_df.iterrows():
    es.index(index="recommendations", body={
        "userId": int(row["userId"]),
        "movieId": int(row["movieId"]),
        "predicted_rating": float(row["predicted_rating"])
    })

spark.stop()