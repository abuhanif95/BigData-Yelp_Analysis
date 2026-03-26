%pyspark
#no 8
from pyspark.sql.functions import udf, size, explode, col, count, regexp_replace, lower, split, collect_list, broadcast
from pyspark.sql.types import ArrayType, StringType
import re

# Fixed tokenize function
def tokenize(text):
    if text is None:
        return []
    # Convert to lowercase and remove non-alphabetic characters
    cleaned = re.sub(r'[^a-z\s]', '', text.lower())
    # Split into words
    words = cleaned.split()
    # Remove empty strings and return words
    return [w for w in words if w.strip() != ""]

# Register the UDF
tokenize_udf = udf(tokenize, ArrayType(StringType()))

print("Processing reviews for word associations...")

# Define stopwords
stopwords = set(["a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "in", "is", "it", "of", "on", "or", "the", "to", "with", "i", "my", "this", "that", "was", "were", "not", "no", "so", "very", "just", "had", "have", "from", "they", "he", "she", "we", "you", "me", "him", "her", "us", "them", "food", "place", "restaurant"])

# Sample reviews to make it manageable
sampled_reviews = spark.table("yelp_review") \
    .select("review_id", "text") \
    .sample(fraction=0.01, seed=42) \
    .limit(5000)

print(f"Processing {sampled_reviews.count()} sampled reviews...")

# Tokenize and explode
review_words = sampled_reviews \
    .select("review_id", explode(tokenize_udf("text")).alias("word"))

# Remove stopwords
review_words = review_words.filter(~col("word").isin(stopwords))

# Get frequent words to limit combinations
frequent_words = review_words \
    .groupBy("word") \
    .agg(count("*").alias("freq")) \
    .filter(col("freq") >= 10) \
    .orderBy(col("freq").desc()) \
    .limit(200) \
    .select("word")

# Filter to only frequent words
filtered_words = review_words \
    .join(broadcast(frequent_words), "word")

# Function to generate pairs from a list of words
def generate_pairs(word_list):
    if not word_list or len(word_list) < 2:
        return []
    pairs = []
    for i in range(len(word_list)):
        for j in range(i+1, len(word_list)):
            # Sort to avoid duplicates
            w1, w2 = sorted([word_list[i], word_list[j]])
            pairs.append((w1, w2))
    return pairs

generate_pairs_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))

print("Generating word pairs...")

# Group words by review and generate pairs
word_pairs = filtered_words \
    .groupBy("review_id") \
    .agg(collect_list("word").alias("words")) \
    .filter(size("words") >= 2) \
    .select(explode(generate_pairs_udf("words")).alias("pair")) \
    .select(col("pair")[0].alias("word1"), col("pair")[1].alias("word2")) \
    .groupBy("word1", "word2") \
    .agg(count("*").alias("co_occurrence")) \
    .orderBy(col("co_occurrence").desc()) \
    .limit(100)

z.show(word_pairs)