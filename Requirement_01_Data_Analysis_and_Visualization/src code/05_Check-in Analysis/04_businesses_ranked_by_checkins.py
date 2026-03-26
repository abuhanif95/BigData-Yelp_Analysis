%pyspark
# Task V.4: Rank all businesses based on check-in counts
print("="*60)
print("Task V.4: Top Businesses by Check-in Counts")
print("="*60)

from pyspark.sql.functions import col, count, desc, row_number

# Count check-ins per business
checkins_per_business = checkins_exploded.groupBy("business_id") \
    .agg(count("*").alias("checkin_count"))

# Join with business info and rank
business_checkins = checkins_per_business.join(
    business_df.select("business_id", "name", "city", "state", "stars", "review_count"),
    "business_id"
).select(
    "name", "city", "state", 
    col("stars").alias("rating"),
    col("review_count").alias("total_reviews"),
    "checkin_count"
).orderBy(desc("checkin_count"))

# Get top 20 businesses
top_businesses = business_checkins.limit(20)

# Display results
top_businesses.createOrReplaceTempView("top_businesses_checkins")
z.show(top_businesses)

# Print summary
print("\nTop 20 Businesses by Check-in Counts:")
print("-" * 90)
print(f"{'Rank':<5} {'Business Name':<35} {'City':<20} {'Check-ins':>12} {'Rating':>8}")
print("-" * 90)

for i, row in enumerate(top_businesses.collect(), 1):
    print(f"{i:<5} {row['name'][:34]:<35} {row['city'][:19]:<20} {row['checkin_count']:>12,} {row['rating']:>8.1f}")

# Get top business
top_business = top_businesses.first()
print(f"\n🏆 Top business by check-ins: {top_business['name']}")
print(f"   City: {top_business['city']}, {top_business['state']}")
print(f"   Check-ins: {top_business['checkin_count']:,}")
print(f"   Rating: {top_business['rating']:.1f} stars")
print(f"   Reviews: {top_business['total_reviews']:,}")

# Calculate check-in to review ratio
if top_business['total_reviews'] > 0:
    ratio = top_business['checkin_count'] / top_business['total_reviews']
    print(f"   Check-in to Review Ratio: {ratio:.1f}:1")