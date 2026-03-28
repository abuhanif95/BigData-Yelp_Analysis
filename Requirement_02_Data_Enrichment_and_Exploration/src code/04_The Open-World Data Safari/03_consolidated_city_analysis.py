%pyspark
from pyspark.sql import functions as F

print("Step 4: Deep dive analysis...")

# Review activity per restaurant by starbucks tier
print("\n=== Reviews Per Restaurant by Starbucks Tier ===")
tier_deep = (
    city_consolidated
    .withColumn("starbucks_tier",
        F.when(F.col("starbucks_count") >= 40, "1_High (40+ Starbucks)")
         .when(F.col("starbucks_count") >= 20, "2_Medium (20-39 Starbucks)")
         .when(F.col("starbucks_count") >= 10, "3_Low (10-19 Starbucks)")
         .otherwise("4_Minimal (< 10 Starbucks)")
    )
    .withColumn("reviews_per_restaurant",
        F.round(F.col("total_reviews") / F.col("total_restaurants"), 1)
    )
    .groupBy("starbucks_tier")
    .agg(
        F.count("*").alias("city_count"),
        F.round(F.avg("avg_restaurant_rating"), 3).alias("avg_rating"),
        F.round(F.avg("total_restaurants"), 0).alias("avg_restaurants"),
        F.round(F.avg("total_reviews"), 0).alias("avg_reviews"),
        F.round(F.avg("reviews_per_restaurant"), 1).alias("avg_reviews_per_restaurant"),
        F.round(F.avg("starbucks_count"), 1).alias("avg_starbucks")
    )
    .orderBy("starbucks_tier")
)
tier_deep.show(truncate=False)
z.show(tier_deep)

# Top vs Bottom cities comparison
print("\n=== Top 5 HIGH Starbucks Cities ===")
top5 = (
    city_consolidated
    .orderBy("starbucks_count", ascending=False)
    .limit(5)
    .withColumn("reviews_per_restaurant",
        F.round(F.col("total_reviews") / F.col("total_restaurants"), 1))
)
top5.show(truncate=False)

print("\n=== Top 5 LOW Starbucks Cities ===")
bottom5 = (
    city_consolidated
    .orderBy("starbucks_count", ascending=True)
    .limit(5)
    .withColumn("reviews_per_restaurant",
        F.round(F.col("total_reviews") / F.col("total_restaurants"), 1))
)
bottom5.show(truncate=False)

# Correlation numbers
print("\n=== CORRELATION RESULTS ===")
corr1 = city_consolidated.stat.corr("starbucks_count", "avg_restaurant_rating")
corr2 = city_consolidated.stat.corr("starbucks_count", "total_reviews")
corr3 = city_consolidated.stat.corr("starbucks_count", "total_restaurants")

print(f"Starbucks Count vs Avg Restaurant Rating : {corr1:.4f}")
print(f"Starbucks Count vs Total Reviews         : {corr2:.4f}")
print(f"Starbucks Count vs Total Restaurants     : {corr3:.4f}")

print("\n✅ Step 4 Done!")