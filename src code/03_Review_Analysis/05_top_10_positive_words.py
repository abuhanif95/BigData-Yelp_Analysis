%pyspark
#no-%
from pyspark.sql.functions import split, explode, lower, regexp_replace, col, count

positive_words = spark.table("yelp_review") \
    .filter(col("stars") > 3) \
    .select(explode(split(regexp_replace(lower(col("text")), "[^a-z ]", ""), "\\s+")).alias("word")) \
    .filter(col("word") != "") \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("word") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc()) \
    .limit(10)

z.show(positive_words)