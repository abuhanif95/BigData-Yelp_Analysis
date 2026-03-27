%pyspark
from pyspark.sql import functions as F

print("Step 4b: Closure rate breakdown by manipulation intensity...")

# --- Segment businesses by post-spike rating drop magnitude ---
closure_by_manipulation = (
    consequence_analysis
    .withColumn("manipulation_tier",
        F.when(F.col("rating_drop_after_spike") >= 1.5, "Severe  (drop >= 1.5 stars)")
         .when(F.col("rating_drop_after_spike") >= 1.0, "Heavy   (drop >= 1.0 stars)")
         .when(F.col("rating_drop_after_spike") >= 0.5, "Moderate(drop >= 0.5 stars)")
         .otherwise(                                     "Light   (drop < 0.5 stars)")
    )
    .groupBy("manipulation_tier")
    .agg(
        F.count("*").alias("business_count"),
        F.round(
            F.sum(F.when(F.col("still_open") == 0, 1).otherwise(0)) /
            F.count("*") * 100, 1
        ).alias("closure_rate_pct"),
        F.round(F.avg("avg_before"),              2).alias("avg_pre_spike_rating"),
        F.round(F.avg("avg_during"),              2).alias("avg_peak_rating"),
        F.round(F.avg("avg_after"),               2).alias("avg_post_spike_rating"),
        F.round(F.avg("rating_drop_after_spike"), 2).alias("avg_drop"),
        F.round(F.avg("net_long_term_change"),    2).alias("avg_net_change")
    )
    .orderBy("closure_rate_pct", ascending=False)
)

print("\n=== Closure Rate by Manipulation Intensity ===")
closure_by_manipulation.show(truncate=False)
z.show(closure_by_manipulation)

# --- Review volume change: did fake boost attract real reviewers? ---
print("\n=== Review Volume Change After Spike ===")
volume_analysis = (
    consequence_analysis
    .withColumn("volume_change_pct",
        F.round(
            (F.col("review_count_after") - F.col("review_count_before")) /
            (F.col("review_count_before") + 1) * 100, 1
        )
    )
    .groupBy(
        F.when(F.col("still_open") == 1, "Open").otherwise("Closed").alias("status")
    )
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("volume_change_pct"),    2).alias("avg_volume_change_pct"),
        F.round(F.avg("review_count_before"),  1).alias("avg_reviews_before"),
        F.round(F.avg("review_count_after"),   1).alias("avg_reviews_after"),
        F.round(F.avg("net_long_term_change"), 2).alias("avg_net_rating_change")
    )
)
volume_analysis.show(truncate=False)
z.show(volume_analysis)