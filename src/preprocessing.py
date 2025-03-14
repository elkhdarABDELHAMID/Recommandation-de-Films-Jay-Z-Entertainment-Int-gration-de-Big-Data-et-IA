from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Preprocessing").getOrCreate()

ratings_df = spark.read.option("delimiter", "\t").csv("/data/u.data", inferSchema=True) \
                       .toDF("userId", "movieId", "rating", "timestamp")

ratings_df = ratings_df.na.drop()

movies_df = spark.read.option("delimiter", "|").csv("/data/u.item", inferSchema=False)

genres = ["unknown", "Action", "Adventure", "Animation", "Childrens", "Comedy", "Crime", "Documentary", "Drama", 
          "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
columns = ["movieId", "title", "release_date", "video_release_date", "imdb_url"] + genres
movies_df = movies_df.toDF(*columns)

movies_df = movies_df.select("movieId", "title", F.array(*genres).alias("genres"))

movies_df = movies_df.withColumn("movieId", F.col("movieId").cast("int"))

combined_df = ratings_df.join(movies_df, "movieId", "inner")

combined_df.write.mode("overwrite").parquet("/data/processed_data.parquet")

spark.stop()