%pyspark

from pyspark.sql.functions import year, to_date, col, count

# Silent users: overall review_count = 0
silent_users = spark.table("yelp_user") \
    .filter(col("review_count") == 0) \
    .select("user_id", "yelping_since")

# Total users joined per year
total_users_by_year = spark.table("yelp_user") \
    .withColumn("join_year", year(to_date(col("yelping_since")))) \
    .groupBy("join_year") \
    .agg(count("*").alias("total_users"))

# Silent users per year
silent_by_year = silent_users \
    .withColumn("join_year", year(to_date(col("yelping_since")))) \
    .groupBy("join_year") \
    .agg(count("*").alias("silent_users"))

# Join and compute proportion
silent_proportion = total_users_by_year.join(silent_by_year, "join_year", "left") \
    .fillna(0) \
    .select("join_year", "total_users", "silent_users") \
    .withColumn("silent_proportion", col("silent_users") / col("total_users")) \
    .orderBy("join_year")

z.show(silent_proportion)