%pyspark

from pyspark.sql.functions import col, explode, year, to_date, avg, min, length, split, size, from_json, when
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

# Step 1: Parse elite string to array and get first elite year for each user
elite_users = spark.table("yelp_user") \
    .filter(col("elite").isNotNull()) \
    .filter(col("elite") != "[]") \
    .withColumn("elite_array", from_json(col("elite"), ArrayType(StringType()))) \
    .select("user_id", explode("elite_array").alias("elite_year")) \
    .groupBy("user_id") \
    .agg(min("elite_year").alias("first_elite_year"))

# Step 2: Join reviews with elite info and classify before/after
reviews_with_elite = spark.table("yelp_review") \
    .join(elite_users, "user_id", "inner") \
    .withColumn("review_year", year(to_date(col("date")))) \
    .withColumn("period", 
                when(col("review_year") < col("first_elite_year"), "before")
                .otherwise("after")) \
    .withColumn("word_count", size(split(col("text"), "\\s+"))) \
    .select("user_id", "period", "word_count", "useful")

# Step 3: Compute averages for each user and period
user_period_avg = reviews_with_elite.groupBy("user_id", "period") \
    .agg(
        avg("word_count").alias("avg_word_count"),
        avg("useful").alias("avg_useful")
    )

# Step 4: Separate before and after data
before_data = user_period_avg.filter(col("period") == "before") \
    .select("user_id", 
            col("avg_word_count").alias("before_word_count"),
            col("avg_useful").alias("before_useful"))

after_data = user_period_avg.filter(col("period") == "after") \
    .select("user_id",
            col("avg_word_count").alias("after_word_count"),
            col("avg_useful").alias("after_useful"))

# Step 5: Join before and after
user_comparison = before_data.join(after_data, "user_id", "inner")

# Step 6: Calculate overall averages
overall_impact = user_comparison.agg(
    avg("before_word_count").alias("avg_word_before"),
    avg("after_word_count").alias("avg_word_after"),
    avg("before_useful").alias("avg_useful_before"),
    avg("after_useful").alias("avg_useful_after")
)

# Step 7: Show results
z.show(overall_impact)

# Optional: Show sample of user-level comparison
z.show(user_comparison.limit(20))