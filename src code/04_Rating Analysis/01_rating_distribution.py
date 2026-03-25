%pyspark
# Task IV.1: Distribution of ratings (1-5 stars)
print("="*60)
print("Task IV.1: Rating Distribution Analysis")
print("="*60)

# Import only what we need - avoid importing round from Spark
from pyspark.sql.functions import col, count, avg

# Calculate rating distribution
rating_distribution = reviews_df.groupBy("stars") \
    .agg(count("*").alias("review_count")) \
    .withColumn("percentage", (col("review_count") / reviews_df.count()) * 100) \
    .orderBy("stars")

# Display results
rating_distribution.createOrReplaceTempView("rating_distribution")
z.show(rating_distribution)

# Print summary statistics using Python arithmetic
print("\nRating Distribution Summary:")
total_reviews = reviews_df.count()

# Collect data to driver
dist_data = rating_distribution.collect()

for row in dist_data:
    stars = row['stars']
    count_val = row['review_count']
    percentage = row['percentage']
    bar = "█" * int(percentage / 2)
    # Using f-string formatting - no round function needed
    print(f"  {stars} star: {count_val:>10,} reviews ({percentage:>5.2f}%) {bar}")

# Calculate cumulative percentages
positive_reviews = reviews_df.filter(col("stars") >= 4).count()
neutral_reviews = reviews_df.filter(col("stars") == 3).count()
negative_reviews = reviews_df.filter(col("stars") <= 2).count()

print("\n" + "="*60)
print("Cumulative Statistics:")
print("="*60)
# Use string formatting with .2f to round instead of round() function
print(f"  Positive reviews (4-5 stars): {positive_reviews:>10,} ({positive_reviews/total_reviews*100:.2f}%)")
print(f"  Neutral reviews (3 stars):    {neutral_reviews:>10,} ({neutral_reviews/total_reviews*100:.2f}%)")
print(f"  Negative reviews (1-2 stars): {negative_reviews:>10,} ({negative_reviews/total_reviews*100:.2f}%)")

# Calculate average rating
avg_rating_result = reviews_df.select(avg("stars")).collect()[0][0]
print(f"\n  Overall average rating: {avg_rating_result:.2f} stars")