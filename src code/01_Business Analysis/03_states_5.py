%pyspark
# Task 3: Top 5 states with most merchants
top_states = business_df.filter(col("state").isNotNull()) \
    .groupBy("state") \
    .agg(count("*").alias("merchant_count")) \
    .orderBy(desc("merchant_count")) \
    .limit(5)

top_states.createOrReplaceTempView("top_states")
z.show(top_states)
