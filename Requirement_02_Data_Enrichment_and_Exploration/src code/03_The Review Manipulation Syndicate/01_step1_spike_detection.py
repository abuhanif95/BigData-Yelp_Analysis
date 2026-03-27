%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

business_df = spark.table("business")
review_df   = spark.table("review")
user_df     = spark.table("users")

print("Step 1: Finding businesses with suspicious 5-star spikes...")

# --- Count 5-star reviews per business per week ---
weekly_5star = (
    review_df
    .filter(F.col("stars") == 5)
    .withColumn("week", F.date_trunc("week", F.col("date")))
    .groupBy("business_id", "week")
    .agg(F.count("*").alias("weekly_5star_count"))
)

# --- Average weekly 5-star reviews per business ---
avg_weekly_5star = (
    weekly_5star
    .groupBy("business_id")
    .agg(
        F.avg("weekly_5star_count").alias("avg_weekly_5star"),
        F.sum("weekly_5star_count").alias("total_5star")
    )
)

# --- Flag spike weeks: 5x above average AND >=10 absolute ---
spike_businesses = (
    weekly_5star
    .join(avg_weekly_5star, "business_id")
    .filter(F.col("weekly_5star_count") >= F.col("avg_weekly_5star") * 5)
    .filter(F.col("avg_weekly_5star") >= 1)
    .filter(F.col("weekly_5star_count") >= 10)
    .groupBy("business_id")
    .agg(
        F.count("week").alias("spike_weeks"),
        F.max("weekly_5star_count").alias("max_spike_count"),
        F.min("week").alias("first_spike_week")
    )
)

suspicious_businesses = (
    spike_businesses
    .join(business_df.select("business_id", "name", "city", "state",
                              "stars", "review_count"), "business_id")
    .join(avg_weekly_5star, "business_id")
    .select(
        "name", "city", "state",
        F.col("stars").alias("current_rating"),
        F.col("review_count").alias("total_reviews"),
        "spike_weeks",
        "max_spike_count",
        F.round("avg_weekly_5star", 2).alias("normal_weekly_avg"),
        "first_spike_week"
    )
    .orderBy("max_spike_count", ascending=False)
)

# Cache for reuse across cells
suspicious_businesses.cache()

print(f"\nSuspicious businesses found: {suspicious_businesses.count()}")
print("\n=== Top 20 Suspicious Businesses ===")
suspicious_businesses.show(20, truncate=False)
z.show(suspicious_businesses)