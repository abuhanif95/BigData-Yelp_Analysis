%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

business_df = spark.table("business")
review_df   = spark.table("review")
user_df     = spark.table("users")

print("Step 2a: Identifying ghost account characteristics...")

# --- Rebuild spike biz IDs (shared dependency) ---
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
spike_biz_ids = (
    weekly_5star
    .join(avg_weekly_5star, "business_id")
    .filter(F.col("weekly_5star_count") >= F.col("avg_weekly_5star") * 5)
    .filter(F.col("avg_weekly_5star") >= 1)
    .filter(F.col("weekly_5star_count") >= 10)
    .select("business_id")
    .distinct()
)

# --- Reviews during spikes at suspicious businesses ---
suspicious_reviews = (
    review_df
    .join(spike_biz_ids, "business_id")
    .filter(F.col("stars") == 5)
    .join(
        user_df.select("user_id", "name", "review_count",
                       "yelping_since", "fans", "average_stars"),
        "user_id"
    )
)

# --- Score each reviewer on 4 ghost indicators ---
ghost_accounts = (
    suspicious_reviews
    .withColumn("account_age_days",
        F.datediff(F.col("date"), F.to_date(F.col("yelping_since"))))
    .withColumn("is_new_account",
        F.when(F.col("account_age_days") <= 180, 1).otherwise(0))
    .withColumn("is_low_activity",
        F.when(F.col("review_count") <= 5, 1).otherwise(0))
    .withColumn("is_five_star_only",
        F.when(F.col("average_stars") >= 4.8, 1).otherwise(0))
    .withColumn("is_no_fans",
        F.when(F.col("fans") == 0, 1).otherwise(0))
    .withColumn("ghost_score",
        F.col("is_new_account") + F.col("is_low_activity") +
        F.col("is_five_star_only") + F.col("is_no_fans"))
)

ghost_accounts.cache()

# --- Aggregate indicator rates ---
print("\n=== Ghost Account Indicator Summary ===")
ghost_summary = ghost_accounts.agg(
    F.count("*").alias("total_suspicious_reviews"),
    F.round(F.avg("is_new_account")    * 100, 2).alias("pct_new_accounts_under6mo"),
    F.round(F.avg("is_low_activity")   * 100, 2).alias("pct_low_activity_under5"),
    F.round(F.avg("is_five_star_only") * 100, 2).alias("pct_5star_only_accounts"),
    F.round(F.avg("is_no_fans")        * 100, 2).alias("pct_no_fans_accounts"),
    F.round(F.avg("ghost_score"),       2).alias("avg_ghost_score")
)
ghost_summary.show(truncate=False)
z.show(ghost_summary)

# --- Top businesses with highest ghost review concentration ---
print("\n=== High Ghost Score Reviewers (Score >= 3) ===")
high_ghost = (
    ghost_accounts
    .filter(F.col("ghost_score") >= 3)
    .groupBy("business_id")
    .agg(
        F.count("*").alias("ghost_review_count"),
        F.round(F.avg("ghost_score"), 2).alias("avg_ghost_score")
    )
    .join(business_df.select("business_id", "name", "city", "state"), "business_id")
    .select("name", "city", "state", "ghost_review_count", "avg_ghost_score")
    .orderBy("ghost_review_count", ascending=False)
)
high_ghost.show(20, truncate=False)
z.show(high_ghost)