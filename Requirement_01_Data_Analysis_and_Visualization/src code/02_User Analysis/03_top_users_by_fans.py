%pyspark

top_fans = spark.table("yelp_user") \
    .select("user_id", "name", "fans") \
    .orderBy(col("fans").desc()) \
    .limit(10)

z.show(top_fans)