%pyspark
# Task 12: Find businesses that received the highest number of 5-star ratings
# This helps identify the most beloved businesses based on customer satisfaction

# Import required functions
from pyspark.sql.functions import col, count, desc, round

# Configure Spark settings to avoid HDFS replication issues
spark.conf.set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
spark.conf.set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "false")

print("="*60)
print("Task 12: Top 20 Merchants with Most Five-Star Reviews")
print("="*60)

# Load business data if not already loaded
print("\n1. Loading business data...")
try:
    # Check if business_df exists, if not load it
    try:
        business_df.count()
        print("   ✓ Business data already loaded")
    except:
        business_df = spark.read.json("/user/hanif/yelp/business")
        business_df.cache()
        print(f"   ✓ Business data loaded. Total businesses: {business_df.count()}")
except Exception as e:
    print(f"   ✗ Error loading business data: {e}")
    raise

# Load reviews data if not already loaded
print("\n2. Loading reviews data...")
try:
    try:
        reviews_df.count()
        print("   ✓ Reviews data already loaded")
    except:
        reviews_df = spark.read.json("/user/hanif/yelp/review")
        reviews_df.cache()
        print(f"   ✓ Reviews data loaded. Total reviews: {reviews_df.count()}")
except Exception as e:
    print(f"   ✗ Error loading reviews data: {e}")
    raise

# Find top merchants with most five-star reviews
print("\n3. Finding top merchants with most 5-star reviews...")

# Filter only 5-star reviews, count per business, join with business names
top_five_star_merchants = reviews_df.filter(col("stars") == 5) \
    .groupBy("business_id") \
    .agg(count("*").alias("five_star_count")) \
    .join(business_df.select("business_id", "name", "city", "state", "stars"), "business_id") \
    .select("name", 
            "city", 
            "state", 
            col("stars").alias("avg_rating"),
            "five_star_count") \
    .orderBy(desc("five_star_count")) \
    .limit(20)

# Display results
print("\n4. Displaying top 20 merchants with most 5-star reviews...")
top_five_star_merchants.createOrReplaceTempView("top_five_star_merchants")
z.show(top_five_star_merchants)

# Print summary statistics
total_merchants = top_five_star_merchants.count()
print(f"\n" + "="*60)
print("RESULTS SUMMARY")
print("="*60)
print(f"Top 20 merchants with most 5-star reviews found and displayed")
print(f"\nTop 5 merchants:")
top_five_star_merchants.select("name", "city", "state", "five_star_count").show(5, truncate=False)

# Additional statistics
total_five_star_reviews = reviews_df.filter(col("stars") == 5).count()
print(f"\nTotal 5-star reviews in entire dataset: {total_five_star_reviews}")

# Calculate percentage of total 5-star reviews from top 20 merchants
top_20_total = top_five_star_merchants.agg(sum("five_star_count")).collect()[0][0]
percentage = (top_20_total / total_five_star_reviews) * 100
print(f"5-star reviews from top 20 merchants: {top_20_total}")
print(f"Percentage of all 5-star reviews: {round(percentage, 2)}%")