%pyspark
# Task IV.5: Rating differential - Compare each merchant against city average
print("="*60)
print("Task IV.5: Rating Differential Analysis")
print("="*60)

from pyspark.sql.functions import col, count, avg, desc

# Calculate city average ratings
city_avg_ratings = reviews_df.join(business_df.select("business_id", "city", "state"), "business_id") \
    .groupBy("city", "state") \
    .agg(
        avg("stars").alias("city_avg"),
        count("*").alias("city_review_count")
    ) \
    .filter(col("city_review_count") >= 50)

# Join to get rating differential
business_with_city_avg = business_df.select("business_id", "name", "city", "state", "stars", "review_count") \
    .join(city_avg_ratings, ["city", "state"]) \
    .withColumn("rating_differential", col("stars") - col("city_avg")) \
    .filter(col("review_count") >= 20)

# Show businesses with highest positive differential
print("\n" + "="*60)
print("Top 20 Businesses that OUTPERFORM their city average:")
print("="*60)
top_performers = business_with_city_avg \
    .orderBy(desc("rating_differential")) \
    .select("name", "city", "state", "stars", "city_avg", "rating_differential", "review_count") \
    .limit(20)

top_performers.createOrReplaceTempView("top_performers")
z.show(top_performers)

# Print top 10
print("\nTop 10 Outperformers:")
for i, row in enumerate(top_performers.collect()[:10], 1):
    print(f"  {i:2}. {row['name']:45} | {row['city']:15} | {row['stars']:.1f} vs {row['city_avg']:.1f} (+{row['rating_differential']:.1f}) | {row['review_count']:,} reviews")

# Show businesses with highest negative differential
print("\n" + "="*60)
print("Top 20 Businesses that UNDERPERFORM their city average:")
print("="*60)
bottom_performers = business_with_city_avg \
    .orderBy("rating_differential") \
    .select("name", "city", "state", "stars", "city_avg", "rating_differential", "review_count") \
    .limit(20)

bottom_performers.createOrReplaceTempView("bottom_performers")
z.show(bottom_performers)

# Print bottom 10
print("\nTop 10 Underperformers:")
for i, row in enumerate(bottom_performers.collect()[:10], 1):
    print(f"  {i:2}. {row['name']:45} | {row['city']:15} | {row['stars']:.1f} vs {row['city_avg']:.1f} ({row['rating_differential']:.1f}) | {row['review_count']:,} reviews")

# Summary statistics
stats = business_with_city_avg.select(
    avg("rating_differential").alias("avg_diff"),
    stddev("rating_differential").alias("stddev_diff")
).collect()[0]

avg_diff = stats['avg_diff']
stddev_diff = stats['stddev_diff'] if stats['stddev_diff'] else 0

print(f"\n" + "="*60)
print("Summary Statistics:")
print("="*60)
print(f"  Average rating differential across all businesses: {avg_diff:.2f} stars")
print(f"  Standard deviation of rating differential: {stddev_diff:.2f}")
print(f"  Total businesses analyzed: {business_with_city_avg.count():,}")

# Find extreme cases
extreme_positive = business_with_city_avg.orderBy(desc("rating_differential")).first()
extreme_negative = business_with_city_avg.orderBy("rating_differential").first()
print(f"\n  Most positive outlier: {extreme_positive['name']} ({extreme_positive['rating_differential']:+.1f} stars)")
print(f"  Most negative outlier: {extreme_negative['name']} ({extreme_negative['rating_differential']:+.1f} stars)")