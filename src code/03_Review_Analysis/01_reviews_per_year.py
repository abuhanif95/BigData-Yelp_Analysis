%pyspark

from pyspark.sql.functions import year, to_date, col, count

reviews_per_year = spark.table("yelp_review") \
    .withColumn("year", year(to_date(col("date")))) \
    .groupBy("year") \
    .agg(count("*").alias("num_reviews")) \
    .orderBy("year")

z.show(reviews_per_year)