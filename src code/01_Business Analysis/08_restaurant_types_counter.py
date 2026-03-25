%pyspark
# Task 8: Find the top 10 category pairs that most frequently appear together in the same business
# This helps identify which business categories complement each other (e.g., Cafes & Bookstores)
from pyspark.sql.functions import col, udf, explode, split, count, desc
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations

# Function to generate all unique pairs of categories from a business
def get_category_pairs(categories_str):
    # If categories is null or empty, return empty list
    if categories_str is None:
        return []
    # Split the categories string into individual categories
    categories = categories_str.split(", ")
    # Need at least 2 categories to form a pair
    if len(categories) < 2:
        return []
    # Generate all unique combinations of 2 categories and format them
    pairs = []
    for pair in combinations(sorted(categories), 2):
        pairs.append(f"{pair[0]}||{pair[1]}")
    return pairs

# Register the UDF (User Defined Function) for Spark
get_pairs_udf = udf(get_category_pairs, ArrayType(StringType()))

# Generate category pairs by exploding the pairs from each business
category_pairs = business_df.filter(col("categories").isNotNull()) \
    .select(explode(get_pairs_udf(col("categories"))).alias("category_pair")) \
    .groupBy("category_pair") \
    .agg(count("*").alias("cooccurrence_count")) \
    .orderBy(desc("cooccurrence_count")) \
    .limit(10)

# Split the concatenated pairs into two separate columns for better readability
category_pairs = category_pairs.withColumn("category1", split(col("category_pair"), "\\|\\|")[0]) \
    .withColumn("category2", split(col("category_pair"), "\\|\\|")[1]) \
    .select("category1", "category2", "cooccurrence_count")

category_pairs.createOrReplaceTempView("category_pairs")
z.show(category_pairs)