%pyspark
from pyspark.sql import functions as F

biz = spark.read.json("hdfs:///user/hanif/yelp/business") \
    .filter(F.col("state").isin("PA", "FL", "LA"))

# PASTE ONE ADDRESS FROM EPIC 1 RESULT HERE
target_address = "3131 Walnut St"

flaw_check = biz \
    .filter(F.col("address") == target_address) \
    .select("name", "stars", "attributes", "categories", "is_open")

print(f"Location DNA for: {target_address}")
z.show(flaw_check)