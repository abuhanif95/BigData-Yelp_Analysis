%pyspark
# Task 10: Analyze how ratings are distributed for Chinese, American, and Mexican restaurants
# This helps understand if certain cuisines get higher ratings or if ratings are polarized
from pyspark.sql.functions import col, count, avg, lit, round

# Define restaurant types to analyze
restaurant_types = ["Chinese", "American", "Mexican"]

# List to store rating data for all cuisines
all_cuisines = []

# Loop through each cuisine type
for cuisine in restaurant_types:
    # Get business IDs for this cuisine
    business_ids = business_df.filter(
        col("categories").contains(cuisine) & 
        col("categories").contains("Restaurants")
    ).select("business_id")
    
    # Get reviews for these businesses and add cuisine label
    cuisine_reviews = reviews_df.join(business_ids, "business_id") \
        .select("stars", lit(cuisine).alias("cuisine"))
    all_cuisines.append(cuisine_reviews)
    
    # Display individual distribution for this cuisine
    print(f"\nRating distribution for {cuisine} restaurants:")
    rating_dist = cuisine_reviews.groupBy("stars") \
        .agg(count("*").alias("count")) \
        .orderBy("stars")
    rating_dist.show()

# Combine all cuisine data for comparison
combined_dist = all_cuisines[0]
for i in range(1, len(all_cuisines)):
    combined_dist = combined_dist.union(all_cuisines[i])

# Calculate the combined distribution
combined_result = combined_dist.groupBy("cuisine", "stars") \
    .agg(count("*").alias("count")) \
    .orderBy("cuisine", "stars")

combined_result.createOrReplaceTempView("combined_rating_dist")
z.show(combined_result)