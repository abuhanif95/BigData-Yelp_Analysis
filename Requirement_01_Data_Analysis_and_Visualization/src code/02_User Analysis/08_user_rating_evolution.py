%pyspark

from pyspark.sql.functions import col, min, avg, datediff, when, count as _count
from pyspark.sql.window import Window

# Step 1: Get first review date for each user
first_review = spark.table("yelp_review") \
    .groupBy("user_id") \
    .agg(min("date").alias("first_review_date"))

# Step 2: Join reviews with first date, classify into periods
reviews_with_period = spark.table("yelp_review") \
    .join(first_review, "user_id") \
    .withColumn("days_diff", datediff(col("date"), col("first_review_date"))) \
    .withColumn("period",
                when((col("days_diff") >= 0) & (col("days_diff") <= 365), "first_year")
                .when((col("days_diff") >= 2*365) & (col("days_diff") <= 3*365), "third_year")
                .otherwise("other"))

# Step 3: Keep only users who have both first and third year reviews
user_period_avg = reviews_with_period \
    .filter(col("period") != "other") \
    .groupBy("user_id", "period") \
    .agg(avg("stars").alias("avg_stars")) \
    .groupBy("user_id") \
    .pivot("period") \
    .agg(avg("avg_stars")) \
    .filter(col("first_year").isNotNull() & col("third_year").isNotNull())

# Step 4: Calculate overall averages across users
overall_evolution = user_period_avg.agg(
    avg("first_year").alias("avg_first_year"),
    avg("third_year").alias("avg_third_year")
)

z.show(overall_evolution)
# Optionally, show individual user data
# z.show(user_period_avg.limit(20))