%pyspark

from pyspark.sql.functions import explode, col, count, year, to_date, from_json
from pyspark.sql.types import ArrayType, StringType

# Convert the string `elite` to an array (if it is a JSON array string)
elite_df = spark.table("yelp_user") \
    .filter(col("elite").isNotNull()) \
    .withColumn("elite_array", from_json(col("elite"), ArrayType(StringType())))

# Explode the array to get one row per elite year
elite_years = elite_df \
    .select(explode("elite_array").alias("elite_year")) \
    .groupBy("elite_year") \
    .agg(count("*").alias("elite_count"))

# Total users joined per year
total_users = spark.table("yelp_user") \
    .withColumn("join_year", year(to_date(col("yelping_since")))) \
    .groupBy("join_year") \
    .agg(count("*").alias("total_users"))

# Join and compute ratio
elite_ratio = elite_years.join(total_users, elite_years.elite_year == total_users.join_year, "inner") \
    .selectExpr("elite_year as year", "elite_count", "total_users", "elite_count / total_users as elite_ratio") \
    .orderBy("year")

z.show(elite_ratio)