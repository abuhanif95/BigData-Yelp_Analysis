%pyspark
from pyspark.sql import functions as F

print("Step 2: Filtering US Starbucks and matching Yelp cities...")

# Filter US only Starbucks
us_starbucks = (
    starbucks_df
    .filter(F.col("Country") == "US")
    .select(
        F.col("City").alias("sbux_city"),
        F.col("State/Province").alias("sbux_state"),
        F.col("Longitude").alias("sbux_lon"),
        F.col("Latitude").alias("sbux_lat")
    )
)

# Count Starbucks per city
sbux_per_city = (
    us_starbucks
    .groupBy("sbux_city", "sbux_state")
    .agg(F.count("*").alias("starbucks_count"))
)

# Get Yelp cities - restaurants only
yelp_cities = (
    business_df
    .filter(F.col("categories").like("%Restaurant%"))
    .groupBy("city", "state")
    .agg(
        F.count("*").alias("restaurant_count"),
        F.round(F.avg("stars"), 2).alias("avg_rating"),
        F.sum("review_count").alias("total_reviews")
    )
)

# Join Starbucks count with Yelp cities
city_analysis = (
    yelp_cities
    .join(
        sbux_per_city,
        (F.lower(F.col("city")) == F.lower(F.col("sbux_city"))) &
        (F.col("state") == F.col("sbux_state"))
    )
    .select(
        "city", "state",
        "starbucks_count",
        "restaurant_count",
        "avg_rating",
        "total_reviews"
    )
    .orderBy("starbucks_count", ascending=False)
)

city_analysis.cache()

print(f"\n✅ Cities matched: {city_analysis.count()}")
print("\n=== Starbucks Count vs Restaurant Ratings by City ===")
city_analysis.show(20, truncate=False)
z.show(city_analysis)