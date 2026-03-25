%pyspark

from pyspark.sql.functions import col

top_reviewers = spark.table("yelp_user") \
    .select("user_id", "name", "review_count") \
    .orderBy(col("review_count").desc()) \
    .limit(10)

z.show(top_reviewers)