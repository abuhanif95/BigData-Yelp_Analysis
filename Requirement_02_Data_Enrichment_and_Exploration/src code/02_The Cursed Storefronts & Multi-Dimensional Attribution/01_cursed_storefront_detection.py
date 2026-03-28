%pyspark
from pyspark.sql import functions as F

biz = spark.read.json("hdfs:///user/hanif/yelp/business") \
    .filter(F.col("state").isin("PA", "FL", "LA"))

cursed_storefronts = biz \
    .groupBy("address", "city", "state") \
    .agg(
        F.count("business_id").alias("tenant_count"),
        F.sum(F.col("is_open").cast("int")).alias("currently_open"),
        F.collect_list("name").alias("previous_tenants")
    ) \
    .filter((F.col("tenant_count") >= 3) & (F.col("currently_open") == 0)) \
    .orderBy(F.desc("tenant_count"))

print("Top 50 Cursed Storefronts:")
z.show(cursed_storefronts.limit(50))