%pyspark
from pyspark.sql import functions as F

business_df = spark.table("business")
review_df   = spark.table("review")

print("Step 2b: NLP frequency analysis on ghost reviews...")

stopwords = set([
    "the","and","for","that","this","with","was","are","you","have",
    "they","from","but","not","been","had","were","will","would","could",
    "their","there","when","what","your","about","also","very","just",
    "more","than","then","into","some","our","out","all","her","his",
    "him","she","has","its","one","can","who","said","get","got","good",
    "great","place","food","back","time","service","here","went","even",
    "really","always","never","every","much","well","like","make","come",
    "been","also","very","just","more","than","then","some","only","both"
])

business_name_words = (
    business_df
    .select(F.lower(F.regexp_replace("name", "[^a-zA-Z ]", " ")).alias("name_clean"))
    .withColumn("word", F.explode(F.split("name_clean", "\\s+")))
    .filter(F.length("word") > 2)
    .select("word")
    .distinct()
)
all_stopwords = stopwords.union(set([row.word for row in business_name_words.collect()]))

ghost_slim = (
    ghost_accounts
    .filter(F.col("ghost_score") >= 3)
    .select(
        F.col("user_id").alias("g_user_id"),
        F.col("business_id").alias("g_business_id"),
        F.col("ghost_score")
    )
)

review_slim = (
    review_df
    .select(
        F.col("user_id").alias("r_user_id"),
        F.col("business_id").alias("r_business_id"),
        F.col("text").alias("review_text")
    )
    .filter(F.col("review_text").isNotNull())
)

ghost_review_text = (
    ghost_slim
    .join(
        review_slim,
        (F.col("g_user_id")     == F.col("r_user_id")) &
        (F.col("g_business_id") == F.col("r_business_id"))
    )
    .select("review_text", "ghost_score")
)

ghost_word_freq = (
    ghost_review_text
    .withColumn("text_clean", F.lower(F.regexp_replace("review_text", "[^a-zA-Z ]", " ")))
    .withColumn("words", F.split("text_clean", "\\s+"))
    .withColumn("word", F.explode("words"))
    .filter(F.length("word") > 3)
    .filter(~F.col("word").isin(all_stopwords))
    .groupBy("word")
    .agg(F.count("*").alias("freq"))
    .orderBy("freq", ascending=False)
)

print("\n=== Top 40 Words in Ghost Reviews (Fraud Vocabulary) ===")
ghost_word_freq.show(40, truncate=False)
z.show(ghost_word_freq.limit(40))