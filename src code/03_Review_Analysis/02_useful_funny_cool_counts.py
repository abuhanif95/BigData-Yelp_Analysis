%pyspark

from pyspark.sql.functions import sum, col

review_votes = spark.table("yelp_review") \
    .agg(
        sum("useful").alias("total_useful"),
        sum("funny").alias("total_funny"),
        sum("cool").alias("total_cool")
    )

z.show(review_votes)