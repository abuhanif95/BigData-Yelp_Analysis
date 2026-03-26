%pyspark
# Task V.3: Identify the most popular city for check-ins
print("="*60)
print("Task V.3: Most Popular City for Check-ins")
print("="*60)

from pyspark.sql.functions import col, count, desc

# Join check-ins with business data to get city information
checkins_with_city = checkins_exploded.join(
    business_df.select("business_id", "name", "city", "state", "latitude", "longitude"), 
    "business_id"
)

# Count check-ins per city
checkins_per_city = checkins_with_city.groupBy("city", "state") \
    .agg(count("*").alias("checkin_count")) \
    .orderBy(desc("checkin_count")) \
    .limit(20)

# Display results
checkins_per_city.createOrReplaceTempView("checkins_per_city")
z.show(checkins_per_city)

# Print summary
print("\nTop 10 Most Popular Cities for Check-ins:")
print("-" * 60)
for i, row in enumerate(checkins_per_city.collect()[:10], 1):
    print(f"  {i:2}. {row['city']:25} {row['state']:2} | {row['checkin_count']:>12,} check-ins")

# Get the most popular city
most_popular_city = checkins_per_city.first()
print(f"\n🏆 Most popular city: {most_popular_city['city']}, {most_popular_city['state']}")
print(f"   Total check-ins: {most_popular_city['checkin_count']:,}")

# Calculate total check-ins
total_checkins_cities = checkins_per_city.agg(sum("checkin_count")).collect()[0][0]
print(f"   Top 20 cities account for {total_checkins_cities:,} check-ins")