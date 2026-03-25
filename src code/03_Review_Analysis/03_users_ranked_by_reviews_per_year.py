%pyspark

from pyspark.sql.functions import year, to_date, col, count, rank
from pyspark.sql.window import Window

# Get reviews per user per year
user_reviews_per_year = spark.table("yelp_review") \
    .withColumn("year", year(to_date(col("date")))) \
    .groupBy("user_id", "year") \
    .agg(count("*").alias("num_reviews"))

# Rank users within each year by number of reviews
window = Window.partitionBy("year").orderBy(col("num_reviews").desc())
ranked = user_reviews_per_year.withColumn("rank", rank().over(window)) \
    .filter(col("rank") <= 10) \
    .orderBy("year", "rank")

z.show(ranked)