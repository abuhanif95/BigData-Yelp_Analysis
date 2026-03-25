%pyspark
# Task 1: Most common merchants in the U.S.
from pyspark.sql.functions import col, count, desc

most_common_merchants = business_df.filter(col("state").isNotNull()) \
    .groupBy("name") \
    .agg(count("*").alias("merchant_count")) \
    .orderBy(desc("merchant_count")) \
    .limit(20)

most_common_merchants.createOrReplaceTempView("most_common_merchants")
z.show(most_common_merchants)
