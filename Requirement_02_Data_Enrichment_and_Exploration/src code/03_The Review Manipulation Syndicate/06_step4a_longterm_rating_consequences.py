%pyspark
from pyspark.sql import functions as F

business_df = spark.table("business")
review_df   = spark.table("review")

print("Step 4a: Analyzing long-term rating consequences...")

# --- Rebuild spike week anchors ---
weekly_5star = (
    review_df
    .filter(F.col("stars") == 5)
    .withColumn("week", F.date_trunc("week", F.col("date")))
    .groupBy("business_id", "week")
    .agg(F.count("*").alias("weekly_5star_count"))
)
avg_weekly_5star = (
    weekly_5star
    .groupBy("business_id")
    .agg(F.avg("weekly_5star_count").alias("avg_weekly_5star"))
)
spike_weeks_df = (
    weekly_5star
    .join(avg_weekly_5star, "business_id")
    .filter(F.col("weekly_5star_count") >= F.col("avg_weekly_5star") * 5)
    .filter(F.col("avg_weekly_5star") >= 1)
    .filter(F.col("weekly_5star_count") >= 10)
    .groupBy("business_id")
    .agg(F.min("week").alias("spike_week"))
)

# --- Ratings BEFORE spike (90 days prior) ---
ratings_before = (
    review_df
    .join(spike_weeks_df, "business_id")
    .filter(
        (F.col("date") >= F.date_sub(F.col("spike_week"), 90)) &
        (F.col("date") <  F.col("spike_week"))
    )
    .groupBy("business_id")
    .agg(
        F.round(F.avg("stars"), 2).alias("avg_before"),
        F.count("*").alias("review_count_before")
    )
)

# --- Ratings DURING spike (30 days) ---
ratings_during = (
    review_df
    .join(spike_weeks_df, "business_id")
    .filter(
        (F.col("date") >= F.col("spike_week")) &
        (F.col("date") <  F.date_add(F.col("spike_week"), 30))
    )
    .groupBy("business_id")
    .agg(
        F.round(F.avg("stars"), 2).alias("avg_during"),
        F.count("*").alias("review_count_during")
    )
)

# --- Ratings AFTER spike (days 30-120) ---
ratings_after = (
    review_df
    .join(spike_weeks_df, "business_id")
    .filter(
        (F.col("date") >= F.date_add(F.col("spike_week"), 30)) &
        (F.col("date") <  F.date_add(F.col("spike_week"), 120))
    )
    .groupBy("business_id")
    .agg(
        F.round(F.avg("stars"), 2).alias("avg_after"),
        F.count("*").alias("review_count_after")
    )
)

# --- Combine all three periods ---
consequence_analysis = (
    ratings_before
    .join(ratings_during, "business_id", "left")
    .join(ratings_after,  "business_id", "left")
    .join(
        business_df.select("business_id", "name", "city", "state", "is_open"),
        "business_id"
    )
    .withColumn("rating_lift_during",
        F.round(F.col("avg_during") - F.col("avg_before"), 2))
    .withColumn("rating_drop_after_spike",
        F.round(F.col("avg_during") - F.col("avg_after"),  2))
    .withColumn("net_long_term_change",
        F.round(F.col("avg_after")  - F.col("avg_before"), 2))
    .select(
        "name", "city", "state",
        F.col("is_open").alias("still_open"),
        "avg_before", "avg_during", "avg_after",
        "rating_lift_during",
        "rating_drop_after_spike",
        "net_long_term_change",
        "review_count_before", "review_count_during", "review_count_after"
    )
    .orderBy("rating_drop_after_spike", ascending=False)
)

consequence_analysis.cache()

print("\n=== Long-term Rating Consequences (Top 20) ===")
consequence_analysis.show(20, truncate=False)
z.show(consequence_analysis)

# --- Overall aggregate summary ---
print("\n=== Overall Consequence Summary ===")
summary = consequence_analysis.agg(
    F.count("*").alias("total_manipulated_businesses"),
    F.round(F.avg("avg_before"),              2).alias("avg_rating_before"),
    F.round(F.avg("avg_during"),              2).alias("avg_rating_during_spike"),
    F.round(F.avg("avg_after"),               2).alias("avg_rating_after"),
    F.round(F.avg("rating_lift_during"),      2).alias("avg_fake_lift"),
    F.round(F.avg("rating_drop_after_spike"), 2).alias("avg_post_spike_drop"),
    F.round(F.avg("net_long_term_change"),    2).alias("avg_net_change"),
    F.sum(F.when(F.col("still_open") == 0, 1).otherwise(0)).alias("now_closed"),
    F.round(
        F.sum(F.when(F.col("still_open") == 0, 1).otherwise(0)) /
        F.count("*") * 100, 2
    ).alias("closure_rate_pct")
)
summary.show(truncate=False)
z.show(summary)