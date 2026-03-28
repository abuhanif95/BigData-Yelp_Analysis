%pyspark
from pyspark.sql import functions as F

print("Step 3: Analyzing Starbucks vs Restaurant ecosystem...")

# Fix city name casing issue - consolidate duplicates
city_consolidated = (
    city_analysis
    .withColumn("city_clean", F.initcap(F.col("city")))
    .groupBy("city_clean", "state")
    .agg(
        F.max("starbucks_count").alias("starbucks_count"),
        F.sum("restaurant_count").alias("total_restaurants"),
        F.round(F.avg("avg_rating"), 2).alias("avg_restaurant_rating"),
        F.sum("total_reviews").alias("total_reviews")
    )
    .orderBy("starbucks_count", ascending=False)
)

city_consolidated.cache()

print("\n=== Consolidated City Analysis ===")
city_consolidated.show(20, truncate=False)
z.show(city_consolidated)

# Categorize cities by Starbucks density
print("\n=== Cities Bucketed by Starbucks Count ===")
bucketed = (
    city_consolidated
    .withColumn("starbucks_tier",
        F.when(F.col("starbucks_count") >= 40, "High (40+ Starbucks)")
         .when(F.col("starbucks_count") >= 20, "Medium (20-39 Starbucks)")
         .when(F.col("starbucks_count") >= 10, "Low (10-19 Starbucks)")
         .otherwise("Minimal (< 10 Starbucks)")
    )
    .groupBy("starbucks_tier")
    .agg(
        F.count("*").alias("city_count"),
        F.round(F.avg("avg_restaurant_rating"), 3).alias("avg_rating"),
        F.round(F.avg("total_restaurants"), 0).alias("avg_restaurants"),
        F.round(F.avg("total_reviews"), 0).alias("avg_reviews"),
        F.round(F.avg("starbucks_count"), 1).alias("avg_starbucks")
    )
    .orderBy("avg_starbucks", ascending=False)
)

bucketed.cache()
print(bucketed.show(truncate=False))
z.show(bucketed)

# Correlation coefficient
print("\n=== Correlation: Starbucks Count vs Avg Restaurant Rating ===")
correlation = city_consolidated.stat.corr(
    "starbucks_count", "avg_restaurant_rating"
)
print(f"Pearson Correlation Coefficient: {correlation:.4f}")

corr2 = city_consolidated.stat.corr(
    "starbucks_count", "total_reviews"
)
print(f"Pearson Correlation (Starbucks vs Total Reviews): {corr2:.4f}")