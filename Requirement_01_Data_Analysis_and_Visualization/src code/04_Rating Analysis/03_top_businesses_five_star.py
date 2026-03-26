%pyspark
# Task IV.3: Top businesses with most five-star ratings
print("="*60)
print("Task IV.3: Top Businesses with Most Five-Star Ratings")
print("="*60)

from pyspark.sql.functions import col, count, desc
import builtins

# Find businesses with most 5-star reviews
top_5star_businesses = reviews_df.filter(col("stars") == 5) \
    .groupBy("business_id") \
    .agg(count("*").alias("five_star_count")) \
    .join(business_df.select("business_id", "name", "city", "state", "stars", "review_count"), "business_id") \
    .select("name", "city", "state", 
            col("stars").alias("overall_rating"),
            col("review_count").alias("total_reviews"),
            "five_star_count") \
    .withColumn("five_star_percentage", (col("five_star_count") / col("total_reviews")) * 100) \
    .orderBy(desc("five_star_count")) \
    .limit(20)

# Display results
top_5star_businesses.createOrReplaceTempView("top_5star_businesses")
z.show(top_5star_businesses)

# Print summary
print("\nTop 10 Businesses with Most 5-Star Reviews:")
for row in top_5star_businesses.collect()[:10]:
    print(f"  {row['name']:40} | {row['city']:20} | {row['five_star_count']:>6,} 5-stars ({row['five_star_percentage']:.1f}%)")

# Calculate statistics using Python's built-in functions
total_5star_reviews = reviews_df.filter(col("stars") == 5).count()

# Use Python list comprehension to extract values and Python's sum
five_star_list = [row['five_star_count'] for row in top_5star_businesses.collect()]
total_5star_from_top20 = builtins.sum(five_star_list)

print(f"\n" + "="*60)
print("Summary Statistics:")
print("="*60)
print(f"  Total 5-star reviews in dataset: {total_5star_reviews:,}")
print(f"  5-star reviews from top 20 businesses: {total_5star_from_top20:,}")

if total_5star_reviews > 0:
    percentage = (total_5star_from_top20 / total_5star_reviews) * 100
    print(f"  Percentage of all 5-star reviews: {percentage:.2f}%")
    print(f"  Average 5-star reviews per top business: {total_5star_from_top20/20:.0f}")

# Additional insights
print(f"\n" + "="*60)
print("Additional Insights:")
print("="*60)

# Find business with highest percentage of 5-star reviews
top_percentage = top_5star_businesses.orderBy(desc("five_star_percentage")).first()
print(f"  Highest 5-star percentage: {top_percentage['name']} ({top_percentage['five_star_percentage']:.1f}%)")

# Find business with highest total reviews among top 20
highest_reviews = top_5star_businesses.orderBy(desc("total_reviews")).first()
print(f"  Most total reviews among top 20: {highest_reviews['name']} ({highest_reviews['total_reviews']:,} reviews)")

# City distribution among top 20
city_dist = top_5star_businesses.groupBy("city").count().orderBy(desc("count"))
print(f"\n  City distribution among top 20:")
for row in city_dist.collect():
    print(f"    {row['city']}: {row['count']} businesses")