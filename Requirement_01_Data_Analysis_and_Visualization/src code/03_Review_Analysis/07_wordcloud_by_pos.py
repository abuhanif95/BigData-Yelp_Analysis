%pyspark

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# Simple heuristic: words that end with common noun suffixes (approximation)
def is_noun(word):
    suffixes = ['ing', 'tion', 'ment', 'ness', 'ity', 'er', 'or', 'ion', 'ance', 'ence']
    return any(word.endswith(suffix) for suffix in suffixes)

is_noun_udf = udf(is_noun, BooleanType())

nouns = spark.table("yelp_review") \
    .select(explode(split(regexp_replace(lower(col("text")), "[^a-z ]", ""), "\\s+")).alias("word")) \
    .filter(col("word") != "") \
    .filter(~col("word").isin(stopwords)) \
    .filter(is_noun_udf(col("word"))) \
    .groupBy("word") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc()) \
    .limit(50)

z.show(nouns)