%pyspark
# Task 6: Top 10 most frequent categories
top_categories = business_df.filter(col("categories").isNotNull()) \
    .select(explode(split(col("categories"), ", ")).alias("category")) \
    .groupBy("category") \
    .agg(count("*").alias("category_count")) \
    .orderBy(desc("category_count")) \
    .limit(10)

top_categories.createOrReplaceTempView("top_categories")
z.show(top_categories)