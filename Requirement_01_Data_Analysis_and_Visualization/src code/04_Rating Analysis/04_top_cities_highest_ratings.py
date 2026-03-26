%pyspark
# Task IV.4: Top 10 cities with highest ratings (minimum 100 reviews)
print("="*60)
print("Task IV.4: Top Cities with Highest Ratings")
print("="*60)

from pyspark.sql.functions import col, count, avg, stddev, desc

# Calculate average rating per city
city_ratings = reviews_df.join(business_df.select("business_id", "city", "state"), "business_id") \
    .groupBy("city", "state") \
    .agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_rating"),
        stddev("stars").alias("rating_stddev")
    ) \
    .filter(col("review_count") >= 100) \
    .orderBy(desc("avg_rating")) \
    .limit(10)

# Display results
city_ratings.createOrReplaceTempView("city_ratings")
z.show(city_ratings)

# Print summary using Python
print("\nTop 10 Cities with Highest Average Ratings (min 100 reviews):")
for row in city_ratings.collect():
    avg_rating = row['avg_rating']
    stddev_val = row['rating_stddev'] if row['rating_stddev'] else 0
    print(f"  {row['city']:25} {row['state']:2} | {avg_rating:.2f} stars (±{stddev_val:.2f}) | {row['review_count']:,} reviews")

# Also show bottom cities for comparison
bottom_cities = reviews_df.join(business_df.select("business_id", "city", "state"), "business_id") \
    .groupBy("city", "state") \
    .agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_rating")
    ) \
    .filter(col("review_count") >= 100) \
    .orderBy("avg_rating") \
    .limit(5)

print("\nCities with Lowest Ratings (for comparison):")
for row in bottom_cities.collect():
    avg_rating = row['avg_rating']
    print(f"  {row['city']:25} {row['state']:2} | {avg_rating:.2f} stars | {row['review_count']:,} reviews")