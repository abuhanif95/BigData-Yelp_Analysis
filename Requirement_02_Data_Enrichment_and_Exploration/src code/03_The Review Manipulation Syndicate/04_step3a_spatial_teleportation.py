%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

business_df = spark.table("business")
review_df   = spark.table("review")

print("Step 3a: Detecting spatial teleportation...")

# --- Reviews enriched with business location ---
reviews_with_location = (
    review_df
    .join(business_df.select("business_id", "city", "state"), "business_id")
    .select("user_id", "business_id", "date", "stars", "city", "state")
    .withColumn("date_ts", F.to_timestamp("date"))
)

# --- Lag window: get previous review city/time per user ---
user_window = Window.partitionBy("user_id").orderBy("date_ts")

reviews_ordered = (
    reviews_with_location
    .withColumn("prev_city",  F.lag("city",    1).over(user_window))
    .withColumn("prev_state", F.lag("state",   1).over(user_window))
    .withColumn("prev_date",  F.lag("date_ts", 1).over(user_window))
    .withColumn("hours_since_last",
        (F.col("date_ts").cast("long") -
         F.col("prev_date").cast("long")) / 3600)
)

# --- Flag: different city within 2 hours ---
teleportation = (
    reviews_ordered
    .filter(F.col("prev_city").isNotNull())
    .filter(F.col("city") != F.col("prev_city"))
    .filter(F.col("hours_since_last") <= 2)
    .filter(F.col("hours_since_last") >= 0)
)

print(f"\nSuspicious teleportation events: {teleportation.count()}")

# --- Top teleporting users ---
teleportation_summary = (
    teleportation
    .groupBy("user_id")
    .agg(
        F.count("*").alias("teleportation_count"),
        F.round(F.min("hours_since_last"), 3).alias("min_hours_between"),
        F.collect_set("city").alias("cities_visited")
    )
    .orderBy("teleportation_count", ascending=False)
    .limit(30)
)

print("\n=== Top Teleporting Users ===")
teleportation_summary.show(30, truncate=False)
z.show(teleportation_summary)

# --- Cross-check: how many teleporters are also ghost reviewers? ---
teleporter_ids = teleportation_summary.select("user_id")
ghost_teleporters = (
    ghost_accounts
    .filter(F.col("ghost_score") >= 3)
    .select("user_id")
    .distinct()
    .join(teleporter_ids, "user_id")
)
print(f"\nUsers who are BOTH teleporters AND high-score ghost accounts: {ghost_teleporters.count()}")
