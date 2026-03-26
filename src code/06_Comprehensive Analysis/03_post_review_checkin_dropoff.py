%pyspark

from pyspark.sql.functions import col, count, sum, round, desc, length, regexp_replace, when, avg, stddev, min, max, explode, split, lit
from pyspark.sql.types import IntegerType

print("=" * 80)
print("TASK 2: Review Conversion Rate Analysis")
print("=" * 80)

# Step 1: Calculate check-in counts per business
print("\nStep 1: Calculating check-in counts...")

# Method 1: Using regexp_replace (works for comma-separated strings)
checkin_summary = spark.table("yelp_checkin") \
    .select("business_id", "date") \
    .withColumn("checkin_count", 
                length(col("date")) - length(regexp_replace(col("date"), ",", "")) + 1) \
    .groupBy("business_id") \
    .agg(sum("checkin_count").alias("total_checkins"))

# Alternative Method (if the above doesn't work, use explode method):
# checkin_summary = spark.table("yelp_checkin") \
#     .select("business_id", explode(split(col("date"), ", ")).alias("checkin_time")) \
#     .groupBy("business_id") \
#     .agg(count("*").alias("total_checkins"))

print(f"Check-in data processed for {checkin_summary.count()} businesses")

# Step 2: Get review counts from business table
business_reviews = spark.table("yelp_business") \
    .select("business_id", "name", "city", "review_count", "stars")

print(f"Business data loaded: {business_reviews.count()} businesses")

# Step 3: Join and get top 100 most checked-in businesses
print("\nStep 2: Calculating conversion rates...")

top_checked_in = checkin_summary \
    .join(business_reviews, "business_id", "inner") \
    .withColumn("conversion_rate", 
                round(col("total_checkins") / col("review_count"), 2)) \
    .orderBy(desc("total_checkins")) \
    .limit(100)

print(f"\nTop 100 most checked-in businesses identified")

# Show results
print("\n" + "=" * 80)
print("Top 100 Most Checked-in Businesses with Conversion Rates:")
print("=" * 80)
z.show(top_checked_in.select("business_id", "name", "city", "review_count", "total_checkins", "conversion_rate"))

# Step 4: Analyze conversion rates
print("\n" + "=" * 80)
print("Conversion Rate Statistics:")
print("=" * 80)

conversion_stats = top_checked_in.agg(
    avg("conversion_rate").alias("avg_conversion_rate"),
    min("conversion_rate").alias("min_conversion_rate"),
    max("conversion_rate").alias("max_conversion_rate"),
    round(stddev("conversion_rate"), 2).alias("stddev_conversion_rate")
)

z.show(conversion_stats)

# Step 5: Categorize businesses by conversion rate
print("\n" + "=" * 80)
print("Businesses by Conversion Rate Category:")
print("=" * 80)

conversion_categories = top_checked_in \
    .withColumn("category",
                when(col("conversion_rate") < 10, "Low Conversion (Few check-ins per review)")
                .when(col("conversion_rate") < 50, "Medium Conversion")
                .otherwise("High Conversion (Many check-ins per review)")) \
    .groupBy("category") \
    .agg(count("*").alias("count")) \
    .orderBy("count")

z.show(conversion_categories)

# Step 6: Show businesses with highest conversion (most visits relative to reviews)
print("\n" + "=" * 80)
print("Top 10 Businesses with Highest Check-in to Review Ratio:")
print("=" * 80)

top_conversion = top_checked_in \
    .orderBy(desc("conversion_rate")) \
    .select("name", "city", "review_count", "total_checkins", "conversion_rate") \
    .limit(10)

z.show(top_conversion)

# Step 7: Show businesses with lowest conversion (more reviews than visits)
print("\n" + "=" * 80)
print("Top 10 Businesses with Lowest Check-in to Review Ratio:")
print("=" * 80)

low_conversion = top_checked_in \
    .orderBy("conversion_rate") \
    .select("name", "city", "review_count", "total_checkins", "conversion_rate") \
    .limit(10)

z.show(low_conversion)

# Step 8: Additional insights
print("\n" + "=" * 80)
print("Additional Insights:")
print("=" * 80)

# Average conversion rate by city
print("\nTop 10 Cities by Average Conversion Rate:")
city_conversion = top_checked_in \
    .groupBy("city") \
    .agg(
        avg("conversion_rate").alias("avg_conversion_rate"),
        count("*").alias("num_businesses")
    ) \
    .filter(col("num_businesses") >= 3) \
    .orderBy(desc("avg_conversion_rate")) \
    .limit(10)

z.show(city_conversion)

# Correlation between rating and conversion rate
print("\nCorrelation between Rating and Conversion Rate:")
correlation = top_checked_in.stat.corr("stars", "conversion_rate")
print(f"Correlation coefficient: {correlation:.3f}")

if correlation > 0.3:
    print("✓ Positive correlation: Higher rated businesses tend to have higher conversion rates")
elif correlation < -0.3:
    print("✗ Negative correlation: Higher rated businesses tend to have lower conversion rates")
else:
    print("→ Weak correlation: Rating doesn't strongly predict conversion rate")

# Summary statistics by category
print("\n" + "=" * 80)
print("Summary Statistics by Category:")
print("=" * 80)

category_stats = top_checked_in \
    .withColumn("category",
                when(col("conversion_rate") < 10, "Low")
                .when(col("conversion_rate") < 50, "Medium")
                .otherwise("High")) \
    .groupBy("category") \
    .agg(
        avg("stars").alias("avg_rating"),
        avg("review_count").alias("avg_reviews"),
        avg("total_checkins").alias("avg_checkins"),
        count("*").alias("count")
    )

z.show(category_stats)