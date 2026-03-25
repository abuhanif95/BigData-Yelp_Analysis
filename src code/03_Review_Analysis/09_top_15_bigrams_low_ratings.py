%pyspark
#no9


from pyspark.sql.functions import col, explode
from pyspark.ml.feature import Tokenizer, NGram

# Filter low-star reviews
low_reviews = spark.table("yelp_review") \
    .filter((col("stars") == 1) | (col("stars") == 2)) \
    .select("text")

# Tokenize
tokenizer = Tokenizer(inputCol="text", outputCol="words")
words_df = tokenizer.transform(low_reviews)

# Generate bigrams
ngram = NGram(n=2, inputCol="words", outputCol="bigrams")
bigrams_df = ngram.transform(words_df) \
    .select(explode("bigrams").alias("bigram")) \
    .filter(col("bigram").isNotNull()) \
    .groupBy("bigram") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc()) \
    .limit(15)

z.show(bigrams_df)