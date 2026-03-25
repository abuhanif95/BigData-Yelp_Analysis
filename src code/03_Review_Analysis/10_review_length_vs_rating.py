%pyspark

from pyspark.sql.functions import length, split, size, avg, col

# Calculate word count per review
review_stats = spark.table("yelp_review") \
    .withColumn("word_count", size(split(col("text"), "\\s+"))) \
    .groupBy("stars") \
    .agg(avg("word_count").alias("avg_word_count")) \
    .orderBy("stars")

z.show(review_stats)