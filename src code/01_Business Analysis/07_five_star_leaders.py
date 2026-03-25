%pyspark
# Task 7: Count number of restaurant types
restaurant_types = ["Chinese", "American", "Mexican"]

for cuisine in restaurant_types:
    count = business_df.filter(
        col("categories").contains(cuisine) & 
        col("categories").contains("Restaurants")
    ).count()
    print(f"Number of {cuisine} restaurants: {count}")

# Create DataFrame for visualization
restaurant_counts = spark.createDataFrame([
    ("Chinese", business_df.filter(col("categories").contains("Chinese") & col("categories").contains("Restaurants")).count()),
    ("American", business_df.filter(col("categories").contains("American") & col("categories").contains("Restaurants")).count()),
    ("Mexican", business_df.filter(col("categories").contains("Mexican") & col("categories").contains("Restaurants")).count())
], ["cuisine", "count"])

restaurant_counts.createOrReplaceTempView("restaurant_counts")
z.show(restaurant_counts)