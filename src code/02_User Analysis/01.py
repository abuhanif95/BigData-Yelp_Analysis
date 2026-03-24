%pyspark

# 1. Number of Users Joining Each Year

df_users = spark.table("users")

df_users_per_year = df_users \
    .withColumn("year", F.year(F.to_date("user_yelping_since"))) \
    .groupBy("year") \
    .agg(F.count("user_id").alias("total_users")) \
    .orderBy("year")

df_users_per_year.show()
z.show(df_users_per_year)