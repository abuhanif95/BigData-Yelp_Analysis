%pyspark
# Task 13: Find businesses with high review volume but high rating variability
# These are "polarizing" businesses that people either love (5 stars) or hate (1 star)

# Import required functions
from pyspark.sql.functions import col, count, stddev, avg, round, desc, when

print("="*60)
print("Task 13: Identify Polarizing Businesses")
print("="*60)

# Load business data if not already loaded
print("\n1. Loading business data...")
try:
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

# Calculate statistics for each business
print("\n3. Calculating business statistics (review count, std deviation, avg rating)...")
business_stats = reviews_df.groupBy("business_id") \
    .agg(
        count("*").alias("review_count"),
        stddev("stars").alias("rating_stddev"),
        avg("stars").alias("avg_rating")
    ) \
    .filter(col("review_count") >= 50)  # Only consider businesses with at least 50 reviews

print(f"   ✓ Found {business_stats.count()} businesses with 50+ reviews")

# Find polarizing businesses (highest standard deviation)
print("\n4. Identifying polarizing businesses (highest rating variability)...")
polarizing_businesses = business_stats \
    .orderBy(desc("rating_stddev")) \
    .limit(20) \
    .join(business_df.select("business_id", "name", "city", "state", "categories"), "business_id") \
    .select("name", 
            "city",
            "state",
            col("review_count"),
            round(col("rating_stddev"), 2).alias("rating_stddev"),
            round(col("avg_rating"), 2).alias("avg_rating"),
            "categories") \
    .orderBy(desc("rating_stddev"))

# Display results
print("\n5. Displaying top 20 polarizing businesses...")
polarizing_businesses.createOrReplaceTempView("polarizing_businesses")
z.show(polarizing_businesses)

# Print summary statistics
print(f"\n" + "="*60)
print("RESULTS SUMMARY")
print("="*60)
print(f"Top 20 polarizing businesses found (highest rating variability)")
print(f"\nTop 5 polarizing businesses:")
polarizing_businesses.select("name", "city", "state", "review_count", "rating_stddev", "avg_rating").show(5, truncate=False)

# Additional analysis: Identify the most polarized rating patterns
print("\n6. Analyzing rating patterns for polarizing businesses...")

# Function to analyze rating distribution for a sample business
sample_business = polarizing_businesses.limit(1).collect()
if sample_business:
    business_id_sample = sample_business[0]['business_id']
    business_name = sample_business[0]['name']
    
    print(f"\nRating distribution for most polarizing business: {business_name}")
    rating_dist = reviews_df.filter(col("business_id") == business_id_sample) \
        .groupBy("stars") \
        .agg(count("*").alias("count")) \
        .orderBy("stars")
    rating_dist.show()

# Count how many businesses have extreme polarization (stddev > 1.5)
extreme_polarization = business_stats.filter(col("rating_stddev") > 1.5).count()
print(f"\nBusinesses with extreme polarization (stddev > 1.5): {extreme_polarization}")

# Analyze categories of polarizing businesses
print("\n7. Analyzing categories of polarizing businesses...")
from pyspark.sql.functions import explode, split

polarizing_categories = polarizing_businesses.filter(col("categories").isNotNull()) \
    .select(explode(split(col("categories"), ", ")).alias("category")) \
    .groupBy("category") \
    .agg(count("*").alias("frequency")) \
    .orderBy(desc("frequency")) \
    .limit(10)

print("\nTop categories among polarizing businesses:")
polarizing_categories.show(truncate=False)