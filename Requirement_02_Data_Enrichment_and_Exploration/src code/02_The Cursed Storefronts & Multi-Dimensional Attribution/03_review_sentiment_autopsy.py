%pyspark
from pyspark.sql import functions as F

reviews = spark.read.json("hdfs:///user/hanif/yelp/review")
biz     = spark.read.json("hdfs:///user/hanif/yelp/business") \
    .filter(F.col("state").isin("PA", "FL", "LA"))

# PASTE TARGET ADDRESS HERE
target_address = "3131 Walnut St"

death_signals = reviews \
    .join(biz, reviews.business_id == biz.business_id) \
    .filter(F.col("address") == target_address) \
    .withColumn("issue_type",
        F.when(F.col("text").rlike("(?i)parking|car|garage|valet"),           "Parking / Access")
         .when(F.col("text").rlike("(?i)hidden|find|entrance|locate"),         "Visibility")
         .when(F.col("text").rlike("(?i)expensive|price|rent|overpriced"),     "Cost / Value")
         .when(F.col("text").rlike("(?i)dirty|smell|roach|health department"), "Hygiene / Health")
         .otherwise("Service / Food")
    )

issue_summary = death_signals \
    .groupBy("issue_type") \
    .count() \
    .orderBy(F.desc("count"))

print(f"Failure breakdown for: {target_address}")
z.show(issue_summary)