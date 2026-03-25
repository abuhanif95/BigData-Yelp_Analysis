%pyspark

from pyspark.sql.functions import year, to_date, col, count, explode, from_json
from pyspark.sql.types import ArrayType, StringType

# New users per year
new_users = spark.table("yelp_user") \
    .withColumn("year", year(to_date(col("yelping_since")))) \
    .groupBy("year") \
    .agg(count("*").alias("new_users"))

# Reviews per year
reviews_per_year = spark.table("yelp_review") \
    .withColumn("year", year(to_date(col("date")))) \
    .groupBy("year") \
    .agg(count("*").alias("num_reviews"))

# Elite users per year (from elite array)
elite_years = spark.table("yelp_user") \
    .filter(col("elite").isNotNull()) \
    .withColumn("elite_array", from_json(col("elite"), ArrayType(StringType()))) \
    .select(explode("elite_array").alias("year")) \
    .groupBy("year") \
    .agg(count("*").alias("elite_users"))

# Tips per year (if tip table exists)
try:
    tips_per_year = spark.table("yelp_tip") \
        .withColumn("year", year(to_date(col("date")))) \
        .groupBy("year") \
        .agg(count("*").alias("num_tips"))
except:
    tips_per_year = None

# Check-ins per year (if checkin table exists)
try:
    # Checkin table has a 'date' column with comma-separated timestamps
    checkins_df = spark.table("yelp_checkin") \
        .selectExpr("explode(split(date, ', ')) as dt") \
        .withColumn("year", year(to_date(col("dt")))) \
        .groupBy("year") \
        .agg(count("*").alias("checkins"))
except:
    checkins_df = None

# Join all together
yearly_stats = new_users.join(reviews_per_year, "year", "full") \
    .join(elite_years, "year", "left") \
    .fillna(0)

if tips_per_year is not None:
    yearly_stats = yearly_stats.join(tips_per_year, "year", "left").fillna(0)
if checkins_df is not None:
    yearly_stats = yearly_stats.join(checkins_df, "year", "left").fillna(0)

yearly_stats = yearly_stats.orderBy("year")
z.show(yearly_stats)