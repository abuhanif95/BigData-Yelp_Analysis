%pyspark
from pyspark.sql import functions as F

print("Step 4: Deep insight analysis...")

# Top vs Bottom Starbucks cities comparison
print("\n=== HIGH Starbucks Cities (40+) ===")
high_sbux = city_consolidated.filter(F.col("starbucks_count") >= 40)
high_sbux.show(truncate=False)

print("\n=== LOW Starbucks Cities (< 10) ===")
low_sbux = city_consolidated.filter(F.col("starbucks_count") < 10)
low_sbux.show(truncate=False)

# Reviews per restaurant ratio
print("\n=== Review Engagement Rate by City ===")
engagement = (
    city_consolidated
    .withColumn("reviews_per_restaurant",
        F.round(F.col("total_reviews") / F.col("total_restaurants"), 1))
    .withColumn("starbucks_tier",
        F.when(F.col("starbucks_count") >= 40, "High (40+)")
         .when(F.col("starbucks_count") >= 20, "Medium (20-39)")
         .when(F.col("starbucks_count") >= 10, "Low (10-19)")
         .otherwise("Minimal (<10)")
    )
    .select(
        "city_clean", "state",
        "starbucks_count",
        "total_restaurants",
        "avg_restaurant_rating",
        "reviews_per_restaurant",
        "starbucks_tier"
    )
    .orderBy("starbucks_count", ascending=False)
)

engagement.cache()
engagement.show(20, truncate=False)
z.show(engagement)

# Summary stats by tier
print("\n=== FINAL SUMMARY: Starbucks Tier vs Restaurant Metrics ===")
final_summary = (
    engagement
    .groupBy("starbucks_tier")
    .agg(
        F.count("*").alias("num_cities"),
        F.round(F.avg("starbucks_count"), 1).alias("avg_starbucks"),
        F.round(F.avg("avg_restaurant_rating"), 3).alias("avg_rating"),
        F.round(F.avg("total_restaurants"), 0).alias("avg_restaurants"),
        F.round(F.avg("reviews_per_restaurant"), 1).alias("avg_reviews_per_restaurant")
    )
    .orderBy("avg_starbucks", ascending=False)
)

final_summary.show(truncate=False)
z.show(final_summary)

# Print correlation summary
print("\n" + "="*50)
print("HYPOTHESIS VALIDATION SUMMARY")
print("="*50)
corr1 = city_consolidated.stat.corr("starbucks_count", "avg_restaurant_rating")
corr2 = city_consolidated.stat.corr("starbucks_count", "total_reviews")
corr3 = city_consolidated.stat.corr("starbucks_count", "total_restaurants")

print(f"Starbucks vs Avg Rating correlation:      {corr1:.4f}")
print(f"Starbucks vs Total Reviews correlation:   {corr2:.4f}")
print(f"Starbucks vs Total Restaurants correlation: {corr3:.4f}")

if corr1 > 0.3:
    print("\n✅ HYPOTHESIS SUPPORTED: More Starbucks = Higher ratings")
elif corr1 < -0.3:
    print("\n❌ HYPOTHESIS REJECTED: More Starbucks = Lower ratings")
else:
    print("\n⚠️ HYPOTHESIS PARTIALLY SUPPORTED: Weak correlation found")
print("="*50)