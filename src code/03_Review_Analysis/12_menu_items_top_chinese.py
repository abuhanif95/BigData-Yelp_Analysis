%pyspark

from pyspark.sql.functions import col, explode, lower, regexp_replace, split, count, desc, from_json, array_contains
from pyspark.sql.types import ArrayType, StringType
import re

print("=" * 60)
print("TASK 12: Most Frequently Mentioned Menu Items in Top Chinese Restaurants")
print("=" * 60)

# Step 1: Parse categories and identify top 5 Chinese restaurants
print("\nStep 1: Identifying top 5 Chinese restaurants...")

# Parse categories string to array (if stored as JSON string)
business_with_categories = spark.table("yelp_business") \
    .filter(col("categories").isNotNull()) \
    .withColumn("categories_array", from_json(col("categories"), ArrayType(StringType()))) \
    .filter(array_contains(col("categories_array"), "Chinese")) \
    .select("business_id", "name", "review_count", "stars") \
    .orderBy(desc("review_count")) \
    .limit(5)

# Show the top 5 Chinese restaurants
print("\nTop 5 Chinese Restaurants (by review count):")
z.show(business_with_categories)

# Step 2: Get reviews for these restaurants
print("\nStep 2: Fetching reviews for these restaurants...")

# Get business IDs
business_ids = [row.business_id for row in business_with_categories.collect()]

# Get all reviews for these businesses
reviews_for_chinese = spark.table("yelp_review") \
    .filter(col("business_id").isin(business_ids)) \
    .select("business_id", "text", "stars")

print(f"Found {reviews_for_chinese.count()} reviews for the top 5 Chinese restaurants")

# Step 3: Define comprehensive food dictionary for Chinese cuisine
food_items = set([
    # Noodles
    "noodles", "chow mein", "lo mein", "ramen", "udon", "rice noodles", "dan dan noodles",
    # Rice dishes
    "rice", "fried rice", "steamed rice", "brown rice", "white rice", "sticky rice", "congee",
    # Dumplings
    "dumplings", "potstickers", "gyoza", "wonton", "shumai", "siu mai", "jiaozi",
    # Meat dishes
    "chicken", "pork", "beef", "shrimp", "duck", "lamb", "pork belly", "barbecue pork", "char siu",
    # Signature dishes
    "kung pao", "kung pao chicken", "mapo tofu", "general tso", "general tso chicken", 
    "orange chicken", "sweet and sour", "sweet and sour chicken", "sesame chicken",
    "mongolian beef", "broccoli beef", "pepper steak", "twice cooked pork",
    # Seafood
    "shrimp", "scallops", "fish", "crab", "lobster", "salt and pepper shrimp",
    # Vegetables and tofu
    "tofu", "eggplant", "broccoli", "bok choy", "chinese broccoli", "green beans",
    # Appetizers
    "spring rolls", "egg rolls", "wonton soup", "hot and sour soup", "egg drop soup",
    # Other
    "hot pot", "dim sum", "bao", "bao buns", "soup dumplings", "xiaolongbao"
])

print(f"\nStep 3: Using dictionary with {len(food_items)} food items")

# Step 4: Function to extract food mentions from text
def extract_food_items(text):
    if text is None:
        return []
    text_lower = text.lower()
    found_items = []
    # Check each food item
    for item in food_items:
        if item in text_lower:
            found_items.append(item)
    return found_items

# Register UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

extract_food_udf = udf(extract_food_items, ArrayType(StringType()))

print("\nStep 4: Extracting food mentions from reviews...")

# Extract food mentions and explode
food_mentions = reviews_for_chinese \
    .select(explode(extract_food_udf("text")).alias("menu_item")) \
    .filter(col("menu_item").isNotNull()) \
    .groupBy("menu_item") \
    .agg(count("*").alias("mention_count")) \
    .orderBy(desc("mention_count")) \
    .limit(20)

# Show the results
print("\nStep 5: Top 20 most frequently mentioned menu items:")
z.show(food_mentions)

# Additional analysis: Show which restaurant has the most reviews
print("\n" + "=" * 60)
print("ADDITIONAL INSIGHTS:")
print("=" * 60)

# Show review distribution across the top 5 restaurants
review_distribution = reviews_for_chinese \
    .groupBy("business_id") \
    .agg(count("*").alias("num_reviews"), avg("stars").alias("avg_rating")) \
    .join(business_with_categories.select("business_id", "name", "review_count"), "business_id") \
    .select("name", "num_reviews", "avg_rating", "review_count") \
    .orderBy(desc("num_reviews"))

print("\nReview distribution across top Chinese restaurants:")
z.show(review_distribution)

# Show rating distribution for the top restaurant
top_restaurant_name = business_with_categories.first()["name"]
print(f"\nRating distribution for the top restaurant: {top_restaurant_name}")

rating_dist = reviews_for_chinese \
    .filter(col("business_id") == business_with_categories.first()["business_id"]) \
    .groupBy("stars") \
    .agg(count("*").alias("count")) \
    .orderBy("stars")

z.show(rating_dist)