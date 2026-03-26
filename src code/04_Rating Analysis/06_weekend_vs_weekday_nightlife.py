%pyspark
# Task IV.6: Weekend vs Weekday satisfaction for Nightlife category
print("="*60)
print("Task IV.6: Weekend vs Weekday Satisfaction - Nightlife Category")
print("="*60)

from pyspark.sql.functions import col, count, avg, stddev, when, dayofweek, date_format, to_date, desc
import builtins

# Filter nightlife businesses
nightlife_businesses = business_df.filter(col("categories").contains("Nightlife")) \
    .select("business_id", "name", "city", "state")

nightlife_count = nightlife_businesses.count()
print(f"\nNightlife businesses found: {nightlife_count:,}")

if nightlife_count > 0:
    # Get reviews for nightlife businesses
    nightlife_reviews = reviews_df.join(nightlife_businesses, "business_id") \
        .withColumn("review_date", to_date(col("date"))) \
        .withColumn("is_weekend", 
            when(dayofweek("review_date").isin([1, 7]), "Weekend")
            .otherwise("Weekday"))
    
    # Calculate weekend vs weekday ratings
    weekend_weekday_analysis = nightlife_reviews.groupBy("is_weekend") \
        .agg(
            count("*").alias("review_count"),
            avg("stars").alias("avg_rating"),
            stddev("stars").alias("rating_stddev")
        )
    
    # Display results
    weekend_weekday_analysis.createOrReplaceTempView("weekend_weekday_analysis")
    z.show(weekend_weekday_analysis)
    
    # Statistical comparison using Python
    weekend_data = weekend_weekday_analysis.filter(col("is_weekend") == "Weekend").collect()
    weekday_data = weekend_weekday_analysis.filter(col("is_weekend") == "Weekday").collect()
    
    if weekend_data and weekday_data:
        weekend_rating = weekend_data[0]['avg_rating']
        weekday_rating = weekday_data[0]['avg_rating']
        weekend_stddev = weekend_data[0]['rating_stddev'] if weekend_data[0]['rating_stddev'] else 0
        weekday_stddev = weekday_data[0]['rating_stddev'] if weekday_data[0]['rating_stddev'] else 0
        weekend_count = weekend_data[0]['review_count']
        weekday_count = weekday_data[0]['review_count']
        
        rating_diff = weekend_rating - weekday_rating
        
        print(f"\n" + "="*60)
        print("CONCLUSION:")
        print("="*60)
        print(f"  Weekend reviews: {weekend_count:,} reviews, Avg Rating: {weekend_rating:.2f} (±{weekend_stddev:.2f})")
        print(f"  Weekday reviews: {weekday_count:,} reviews, Avg Rating: {weekday_rating:.2f} (±{weekday_stddev:.2f})")
        print(f"  Difference: {rating_diff:+.2f} stars")
        
        # Use Python's built-in abs() instead of Spark's abs()
        if rating_diff > 0:
            print(f"\n  📈 Nightlife businesses receive {rating_diff:.2f} stars HIGHER on weekends")
            print(f"  → People tend to enjoy nightlife more on weekends")
        elif rating_diff < 0:
            print(f"\n  📉 Nightlife businesses receive {builtins.abs(rating_diff):.2f} stars LOWER on weekends")
            print(f"  → Weekends might be busier leading to worse service")
        else:
            print(f"\n  → No significant difference between weekend and weekday ratings")
        
        # Calculate percentage difference using Python arithmetic
        pct_diff = (rating_diff / weekday_rating) * 100
        print(f"  Percentage difference: {pct_diff:+.1f}%")
        
        # T-test-like insight
        if builtins.abs(rating_diff) > 0.2:
            print(f"  ⚠️  This difference is practically significant (>0.2 stars)")
        else:
            print(f"  ✅ This difference is not practically significant")
    
    # Detailed day-by-day analysis
    print("\n" + "="*60)
    print("Detailed Day-by-Day Analysis for Nightlife:")
    print("="*60)
    
    daily_nightlife = nightlife_reviews \
        .withColumn("weekday", date_format("review_date", "EEEE")) \
        .groupBy("weekday") \
        .agg(
            count("*").alias("review_count"),
            avg("stars").alias("avg_rating")
        ) \
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
    
    daily_nightlife.createOrReplaceTempView("daily_nightlife")
    z.show(daily_nightlife)
    
    print("\nDaily Breakdown:")
    best_day = None
    worst_day = None
    for row in daily_nightlife.collect():
        print(f"  {row['weekday']:10}: {row['review_count']:>8,} reviews, Avg: {row['avg_rating']:.2f} stars")
        if best_day is None or row['avg_rating'] > best_day['avg_rating']:
            best_day = row
        if worst_day is None or row['avg_rating'] < worst_day['avg_rating']:
            worst_day = row
    
    print(f"\n  Best nightlife day: {best_day['weekday']} ({best_day['avg_rating']:.2f} stars)")
    print(f"  Worst nightlife day: {worst_day['weekday']} ({worst_day['avg_rating']:.2f} stars)")
    
    # Additional insights
    print("\n" + "="*60)
    print("Key Insights for Nightlife Businesses:")
    print("="*60)
    
    # Find day with highest review volume
    highest_volume = daily_nightlife.orderBy(desc("review_count")).first()
    print(f"  Highest review volume: {highest_volume['weekday']} ({highest_volume['review_count']:,} reviews)")
    
    # Find day with lowest review volume
    lowest_volume = daily_nightlife.orderBy("review_count").first()
    print(f"  Lowest review volume: {lowest_volume['weekday']} ({lowest_volume['review_count']:,} reviews)")
    
    # Calculate weekend vs weekday volume using Python
    weekend_days = ["Saturday", "Sunday"]
    weekday_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    
    weekend_volume = 0
    weekday_volume = 0
    
    for row in daily_nightlife.collect():
        if row['weekday'] in weekend_days:
            weekend_volume += row['review_count']
        elif row['weekday'] in weekday_days:
            weekday_volume += row['review_count']
    
    print(f"\n  Weekend review volume: {weekend_volume:,} reviews")
    print(f"  Weekday review volume: {weekday_volume:,} reviews")
    if weekday_volume > 0:
        print(f"  Weekend volume ratio: {weekend_volume/weekday_volume:.2f}x of weekday volume")
    
    # Average rating by day type
    weekend_avg = 0
    weekday_avg = 0
    for row in daily_nightlife.collect():
        if row['weekday'] in weekend_days:
            weekend_avg += row['avg_rating']
        elif row['weekday'] in weekday_days:
            weekday_avg += row['avg_rating']
    
    weekend_avg = weekend_avg / 2
    weekday_avg = weekday_avg / 5
    
    print(f"\n  Weekend average rating: {weekend_avg:.2f} stars")
    print(f"  Weekday average rating: {weekday_avg:.2f} stars")
    print(f"  Weekend vs Weekday: {weekend_avg - weekday_avg:+.2f} stars")
    
else:
    print("No nightlife businesses found in the dataset")