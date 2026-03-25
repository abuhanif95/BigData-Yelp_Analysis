%pyspark
# Task IV.2: Weekly rating frequency analysis
print("="*60)
print("Task IV.2: Weekly Rating Frequency Analysis")
print("="*60)

from pyspark.sql.functions import col, count, avg, stddev, when, dayofweek, date_format, desc

# Add weekday information
reviews_with_weekday = reviews_with_date.withColumn(
    "weekday", 
    when(dayofweek("review_date") == 1, "Sunday")
    .when(dayofweek("review_date") == 2, "Monday")
    .when(dayofweek("review_date") == 3, "Tuesday")
    .when(dayofweek("review_date") == 4, "Wednesday")
    .when(dayofweek("review_date") == 5, "Thursday")
    .when(dayofweek("review_date") == 6, "Friday")
    .otherwise("Saturday")
)

# Count reviews by weekday
weekly_reviews = reviews_with_weekday.groupBy("weekday") \
    .agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_rating"),
        stddev("stars").alias("rating_stddev")
    ) \
    .withColumn("avg_rating", (col("avg_rating"))) \
    .withColumn("rating_stddev", (col("rating_stddev"))) \
    .withColumn("weekday_order", 
        when(col("weekday") == "Monday", 1)
        .when(col("weekday") == "Tuesday", 2)
        .when(col("weekday") == "Wednesday", 3)
        .when(col("weekday") == "Thursday", 4)
        .when(col("weekday") == "Friday", 5)
        .when(col("weekday") == "Saturday", 6)
        .when(col("weekday") == "Sunday", 7)
    ) \
    .orderBy("weekday_order") \
    .drop("weekday_order")

# Display results
weekly_reviews.createOrReplaceTempView("weekly_reviews")
z.show(weekly_reviews)

# Print summary using Python
print("\nWeekly Rating Pattern:")
for row in weekly_reviews.collect():
    avg_rating = row['avg_rating']
    print(f"  {row['weekday']:10}: {row['review_count']:>10,} reviews, Avg: {avg_rating:.2f} stars")

# Find best and worst days
best_day = weekly_reviews.orderBy(desc("avg_rating")).first()
worst_day = weekly_reviews.orderBy("avg_rating").first()
print(f"\n🏆 Best day for ratings: {best_day['weekday']} (Avg: {best_day['avg_rating']:.2f} stars)")
print(f"📉 Worst day for ratings: {worst_day['weekday']} (Avg: {worst_day['avg_rating']:.2f} stars)")