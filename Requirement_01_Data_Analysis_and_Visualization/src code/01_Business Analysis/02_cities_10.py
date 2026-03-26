%pyspark
# Task 2: Top 10 cities with most merchants
from pyspark.sql.functions import col, count, desc, concat, lit

top_cities = business_df.filter(col("state").isNotNull() & col("city").isNotNull()) \
    .groupBy("city", "state") \
    .agg(count("*").alias("merchant_count")) \
    .withColumn("city_state", concat(col("city"), lit(", "), col("state"))) \
    .orderBy(desc("merchant_count")) \
    .limit(10)

top_cities.createOrReplaceTempView("top_cities")
z.show(top_cities)