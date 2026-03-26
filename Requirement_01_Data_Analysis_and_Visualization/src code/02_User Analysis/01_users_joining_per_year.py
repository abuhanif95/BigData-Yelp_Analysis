%pyspark

from pyspark.sql.functions import year, to_date, col, count

users_by_year = spark.table("yelp_user") \
    .withColumn("join_year", year(to_date(col("yelping_since")))) \
    .groupBy("join_year") \
    .agg(count("*").alias("num_users")) \
    .orderBy("join_year")

z.show(users_by_year)
# Click the chart icon for a bar chart.