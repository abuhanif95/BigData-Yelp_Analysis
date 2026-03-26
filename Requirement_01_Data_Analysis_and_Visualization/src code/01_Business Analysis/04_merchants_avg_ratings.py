%pyspark
# Task 4: Most common merchants with average ratings
from pyspark.sql.functions import col, count, desc, avg, round

merchants_avg_rating = business_df.filter(col("state").isNotNull()) \
    .groupBy("name") \
    .agg(
        count("*").alias("location_count"),
        round(avg("stars"), 2).alias("avg_rating")
    ) \
    .orderBy(desc("location_count")) \
    .limit(20)

merchants_avg_rating.createOrReplaceTempView("merchants_avg_rating")
z.show(merchants_avg_rating)