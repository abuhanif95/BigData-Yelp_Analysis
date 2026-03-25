%pyspark

from pyspark.sql.functions import split, explode, lower, regexp_replace, col, count
from pyspark.sql.types import StringType

# Define stopwords (simple list)
stopwords = set(["a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "in", "is", "it", "of", "on", "or", "the", "to", "with", "i", "my", "this", "that", "was", "were", "not", "no", "so", "very", "just", "had", "have", "from", "they", "he", "she", "we", "you", "me", "him", "her", "us", "them"])

# Tokenize, clean, filter stopwords, count
word_counts = spark.table("yelp_review") \
    .select(explode(split(regexp_replace(lower(col("text")), "[^a-z ]", ""), "\\s+")).alias("word")) \
    .filter(col("word") != "") \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("word") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc()) \
    .limit(20)

z.show(word_counts)