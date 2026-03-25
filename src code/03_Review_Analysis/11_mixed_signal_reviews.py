%pyspark
#no11
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import re

# Define positive lexicon
positive_lexicon = set(["great", "amazing", "excellent", "good", "nice", "fantastic", 
                        "wonderful", "perfect", "love", "delicious", "best", "awesome", 
                        "yummy", "fabulous", "superb", "outstanding", "incredible", 
                        "exceptional", "wonderful", "pleasant", "enjoyable"])

# Fixed function - uses Python string operations, not PySpark SQL functions
def contains_positive(text):
    if text is None:
        return False
    # Convert to lowercase and remove punctuation using Python's re
    cleaned = re.sub(r'[^a-z\s]', '', text.lower())
    # Split into words
    words = cleaned.split()
    # Check if any positive word is in the text
    return any(word in positive_lexicon for word in words)

# Register the UDF
contains_positive_udf = udf(contains_positive, BooleanType())

print("Finding mixed-signal reviews (1-2 star reviews with positive keywords)...")

# Find reviews with 1 or 2 stars that contain positive words
mixed_reviews = spark.table("yelp_review") \
    .filter((col("stars") == 1) | (col("stars") == 2)) \
    .filter(contains_positive_udf(col("text"))) \
    .select("review_id", "user_id", "business_id", "stars", "text") \
    .limit(100)

# Show results
z.show(mixed_reviews)

# Also show count
count_mixed = mixed_reviews.count()
print(f"\nFound {count_mixed} mixed-signal reviews (1-2 stars with positive keywords)")