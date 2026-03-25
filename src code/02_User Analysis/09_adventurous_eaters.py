%pyspark

from pyspark.sql.functions import col, explode, count, countDistinct, from_json
from pyspark.sql.types import ArrayType, StringType

# Step 1: Parse categories string to array and explode
# The categories column is a JSON array string like '["Restaurants", "Mexican", "Bars"]'
business_with_categories = spark.table("yelp_business") \
    .filter(col("categories").isNotNull()) \
    .withColumn("categories_array", from_json(col("categories"), ArrayType(StringType()))) \
    .select("business_id", explode("categories_array").alias("category"))

# Step 2: Join reviews with business categories
user_categories = spark.table("yelp_review") \
    .select("user_id", "business_id") \
    .join(business_with_categories, "business_id") \
    .select("user_id", "category")

# Step 3: Count distinct categories per user
user_diversity = user_categories.groupBy("user_id") \
    .agg(countDistinct("category").alias("distinct_cuisines"))

# Step 4: Get users with at least 20 reviews
active_users = spark.table("yelp_review") \
    .groupBy("user_id") \
    .agg(count("*").alias("total_reviews")) \
    .filter(col("total_reviews") >= 20)

# Step 5: Join and rank
adventurous_eaters = user_diversity.join(active_users, "user_id") \
    .orderBy(col("distinct_cuisines").desc()) \
    .limit(50)

z.show(adventurous_eaters)