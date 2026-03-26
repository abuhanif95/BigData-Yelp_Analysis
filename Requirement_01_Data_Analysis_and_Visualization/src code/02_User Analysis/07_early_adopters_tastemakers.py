%pyspark

from pyspark.sql.functions import col, row_number, count
from pyspark.sql.window import Window

# Step 1: Identify restaurants with 4.5+ stars and at least 100 reviews
top_restaurants = spark.table("yelp_business") \
    .filter(col("categories").contains("Restaurants")) \
    .filter(col("stars") >= 4.5) \
    .filter(col("review_count") >= 100) \
    .select("business_id", "name")

# Step 2: Get reviews for these restaurants, add row number per business ordered by date
reviews_with_rank = spark.table("yelp_review") \
    .join(top_restaurants, "business_id", "inner") \
    .select("review_id", "user_id", "business_id", "date") \
    .withColumn("rn", row_number().over(Window.partitionBy("business_id").orderBy("date")))

# Step 3: Keep only the first 5 reviews per business
first_five_reviews = reviews_with_rank.filter(col("rn") <= 5)

# Step 4: Count how many such early reviews each user wrote
early_adopters = first_five_reviews.groupBy("user_id") \
    .agg(count("*").alias("num_early_reviews")) \
    .orderBy(col("num_early_reviews").desc()) \
    .limit(20)

z.show(early_adopters)