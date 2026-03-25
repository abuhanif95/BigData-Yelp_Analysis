%pyspark
# Task 11: Find businesses whose average rating increased by at least 1 star in the last 12 months
# This identifies "turnaround" businesses that have significantly improved

# Import required functions
from pyspark.sql.functions import col, to_date, months_between, current_date, avg, round, desc, lit
from pyspark.sql.types import DateType

# Configure Spark settings to avoid HDFS replication issues
spark.conf.set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
spark.conf.set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")

print("="*50)
print("Starting Task 11: Turnaround Merchants Analysis")
print("="*50)

# Load business data
print("\n1. Loading business data...")
try:
    business_df = spark.read.json("/user/hanif/yelp/business")
    business_count = business_df.count()
    print(f"   ✓ Business data loaded successfully. Total businesses: {business_count}")
except Exception as e:
    print(f"   ✗ Error loading business data: {e}")
    raise

# Load reviews data
print("\n2. Loading reviews data...")
try:
    reviews_df = spark.read.json("/user/hanif/yelp/review")
    reviews_count = reviews_df.count()
    print(f"   ✓ Reviews data loaded successfully. Total reviews: {reviews_count}")
except Exception as e:
    print(f"   ✗ Error loading reviews data: {e}")
    raise

# Cache DataFrames for better performance
business_df.cache()
reviews_df.cache()

print("\n3. Processing reviews data...")
# Add date column to reviews for time-based analysis
reviews_with_date = reviews_df.withColumn("review_date", to_date(col("date")))

# Get current date for comparison
current_date_val = current_date()
print(f"   Current date: {spark.sql('SELECT current_date()').collect()[0][0]}")

# Calculate historical average rating (all time except last 12 months)
print("\n4. Calculating historical average ratings (all time except last 12 months)...")
historical_avg = reviews_with_date.filter(
    months_between(current_date_val, col("review_date")) > 12
).groupBy("business_id") \
 .agg(avg("stars").alias("historical_avg"))

historical_count = historical_avg.count()
print(f"   ✓ Businesses with historical data: {historical_count}")

# Calculate recent average rating (last 12 months only)
print("\n5. Calculating recent average ratings (last 12 months)...")
recent_avg = reviews_with_date.filter(
    months_between(current_date_val, col("review_date")) <= 12
).groupBy("business_id") \
 .agg(avg("stars").alias("recent_avg"))

recent_count = recent_avg.count()
print(f"   ✓ Businesses with recent data: {recent_count}")

# Join historical and recent averages, find businesses with 1+ star improvement
print("\n6. Finding turnaround merchants (1+ star improvement)...")
turnaround_merchants = historical_avg.join(recent_avg, "business_id", "inner") \
    .filter((col("recent_avg") - col("historical_avg")) >= 1) \
    .join(business_df.select("business_id", "name"), "business_id") \
    .select("name", 
            round(col("historical_avg"), 2).alias("historical_avg"),
            round(col("recent_avg"), 2).alias("recent_avg"), 
            round((col("recent_avg") - col("historical_avg")), 2).alias("rating_increase")) \
    .orderBy(desc("rating_increase"))

# Display results
print("\n7. Displaying results...")
turnaround_merchants.createOrReplaceTempView("turnaround_merchants")
z.show(turnaround_merchants)

# Print summary statistics
turnaround_count = turnaround_merchants.count()
print(f"\n" + "="*50)
print(f"RESULTS SUMMARY")
print("="*50)
print(f"Total turnaround merchants found: {turnaround_count}")
print(f"These are businesses that improved their average rating by at least 1 star in the last 12 months")
print("\nTop 5 improvement examples:")
turnaround_merchants.show(5, truncate=False)

# Optional: Show statistics
if turnaround_count > 0:
    avg_increase = turnaround_merchants.select(avg("rating_increase")).collect()[0][0]
    print(f"\nAverage rating increase among turnaround merchants: {round(avg_increase, 2)} stars")