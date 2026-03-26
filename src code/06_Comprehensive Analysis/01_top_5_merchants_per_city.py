%pyspark

from pyspark.sql.functions import col, count, avg, rank, row_number, desc, sum, max, when, explode, split, round, lit
from pyspark.sql.window import Window

print("=" * 80)
print("TASK 1: Top 5 Merchants in Each City (Fixed)")
print("=" * 80)

# Step 1: Get business metrics
business_df = spark.table("yelp_business") \
    .select("business_id", "name", "city", "review_count", "stars") \
    .filter(col("city").isNotNull()) \
    .filter(col("city") != "")

print(f"Total businesses: {business_df.count()}")

# Step 2: Calculate check-in counts efficiently
checkin_df = spark.table("yelp_checkin") \
    .select("business_id", explode(split(col("date"), ", ")).alias("checkin")) \
    .groupBy("business_id") \
    .agg(count("*").alias("total_checkins"))

print(f"Businesses with check-ins: {checkin_df.count()}")

# Step 3: Join and calculate scores
combined = business_df \
    .join(checkin_df, "business_id", "left") \
    .fillna(0, subset=["total_checkins"])

# Step 4: Get max values for normalization
max_reviews = combined.agg(max("review_count")).collect()[0][0]
max_checkins = combined.agg(max("total_checkins")).collect()[0][0]

print(f"\nNormalization factors:")
print(f"  Max reviews: {max_reviews}")
print(f"  Max check-ins: {max_checkins}")

# Step 5: Calculate combined score - FIXED: Use col() comparisons, not Python bool
scored = combined \
    .withColumn("review_score", col("review_count") / lit(max_reviews)) \
    .withColumn("rating_score", col("stars") / lit(5.0)) \
    .withColumn("checkin_score", 
                when(col("total_checkins") > 0, col("total_checkins") / lit(max_checkins))
                .otherwise(0)) \
    .withColumn("combined_score", 
                round(col("review_score") * 0.4 + 
                      col("rating_score") * 0.3 + 
                      col("checkin_score") * 0.3, 4))

# Step 6: Rank within cities
window_spec = Window.partitionBy("city").orderBy(desc("combined_score"))

top_5 = scored \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 5) \
    .select("city", "name", "stars", "review_count", "total_checkins", "combined_score", "rank") \
    .orderBy("city", "rank")

print("\n" + "=" * 80)
print("RESULTS: Top 5 Merchants in Each City")
print("=" * 80)

# Show results (limited to avoid overwhelming output)
z.show(top_5.limit(200))

# Show statistics
print("\n" + "=" * 80)
print("STATISTICS")
print("=" * 80)

# Number of cities with at least 5 businesses
cities_count = top_5.groupBy("city").count().filter(col("count") == 5).count()
print(f"Cities with complete top 5 rankings: {cities_count}")

# Top 10 cities by average combined score
print("\nTop 10 Cities by Average Combined Score:")
top_cities = top_5 \
    .groupBy("city") \
    .agg(avg("combined_score").alias("avg_score")) \
    .orderBy(desc("avg_score")) \
    .limit(10)

z.show(top_cities)

# Top 10 businesses overall
print("\nTop 10 Businesses Overall (by combined score):")
top_businesses = scored \
    .orderBy(desc("combined_score")) \
    .select("name", "city", "stars", "review_count", "total_checkins", "combined_score") \
    .limit(10)

z.show(top_businesses)