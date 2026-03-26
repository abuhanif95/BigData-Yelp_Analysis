%pyspark
# Task 9: Count total reviews for Chinese, American, and Mexican restaurants
# This shows which cuisine types generate the most customer feedback
from pyspark.sql.functions import col, count, lit

# First load reviews data
reviews_df = spark.read.json("/user/hanif/yelp/review")
reviews_df.cache()
print(f"Reviews count: {reviews_df.count()}")

# Define the restaurant types to analyze
restaurant_types = ["Chinese", "American", "Mexican"]

# List to store review counts for each cuisine
cuisine_reviews = []

# Loop through each cuisine type
for cuisine in restaurant_types:
    # Find all business IDs for restaurants of this cuisine type
    business_ids = business_df.filter(
        col("categories").contains(cuisine) & 
        col("categories").contains("Restaurants")
    ).select("business_id")
    
    # Count reviews for these businesses
    review_count = reviews_df.join(business_ids, "business_id").count()
    cuisine_reviews.append((cuisine, review_count))
    print(f"Number of reviews for {cuisine} restaurants: {review_count}")

# Create DataFrame for visualization
cuisine_reviews_df = spark.createDataFrame(cuisine_reviews, ["cuisine", "review_count"])
cuisine_reviews_df.createOrReplaceTempView("cuisine_reviews")
z.show(cuisine_reviews_df)