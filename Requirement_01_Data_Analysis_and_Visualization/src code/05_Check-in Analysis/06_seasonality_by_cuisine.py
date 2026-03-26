# Task V.6: Analyze review seasonality by cuisine
print("="*60)
print("Task V.6: Review Seasonality by Cuisine")
print("="*60)

from pyspark.sql.functions import month, when

# Define seasonal categories
seasonal_categories = {
    "Ice Cream": ["Ice Cream", "Frozen Yogurt", "Gelato"],
    "Soup": ["Soup", "Ramen", "Noodle Soup"],
    "Coffee": ["Coffee", "Coffee & Tea"],
    "Barbecue": ["Barbecue", "BBQ"],
    "Seafood": ["Seafood"]
}

# Define months
months = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

# Process each seasonal category
for category_name, keywords in seasonal_categories.items():
    print(f"\n" + "="*60)
    print(f"Seasonality Analysis: {category_name}")
    print("="*60)
    
    # Find businesses in this category
    category_businesses = business_df.filter(
        col("categories").isNotNull()
    )
    
    # Filter businesses matching any keyword
    condition = None
    for keyword in keywords:
        if condition is None:
            condition = col("categories").contains(keyword)
        else:
            condition = condition | col("categories").contains(keyword)
    
    category_businesses = category_businesses.filter(condition)
    business_count = category_businesses.count()
    
    if business_count == 0:
        print(f"No businesses found for {category_name}")
        continue
    
    print(f"Businesses found: {business_count:,}")
    
    # Get reviews for these businesses
    category_reviews = reviews_df.join(
        category_businesses.select("business_id", "name"), 
        "business_id"
    ).withColumn("review_month", month(to_date(col("date"))))
    
    # Count reviews by month
    monthly_reviews = category_reviews.groupBy("review_month") \
        .agg(count("*").alias("review_count")) \
        .orderBy("review_month")
    
    # Calculate percentage
    total_reviews = monthly_reviews.agg(sum("review_count")).collect()[0][0]
    monthly_reviews = monthly_reviews.withColumn(
        "percentage", 
        round((col("review_count") / total_reviews) * 100, 2)
    )
    
    # Display results
    monthly_reviews.createOrReplaceTempView(f"{category_name.lower().replace(' ', '_')}_seasonality")
    z.show(monthly_reviews)
    
    # Print summary
    print(f"\nMonthly Review Distribution for {category_name}:")
    print("-" * 60)
    print(f"{'Month':<12} {'Reviews':>12} {'Percentage':>12} {'Bar Chart':>20}")
    print("-" * 60)
    
    max_reviews = monthly_reviews.agg(max("review_count")).collect()[0][0]
    
    for row in monthly_reviews.collect():
        month_val = row['review_month']
        month_name = months[month_val]
        reviews = row['review_count']
        percentage = row['percentage']
        bar_length = int((reviews / max_reviews) * 30)
        bar = "█" * bar_length
        print(f"  {month_name:<12} {reviews:>12,} {percentage:>11.1f}% {bar:>20}")
    
    # Find peak and off-peak months
    peak_month = monthly_reviews.orderBy(desc("review_count")).first()
    low_month = monthly_reviews.orderBy("review_count").first()
    
    print(f"\n📈 Peak month: {months[peak_month['review_month']]} ({peak_month['review_count']:,} reviews, {peak_month['percentage']:.1f}%)")
    print(f"📉 Low month: {months[low_month['review_month']]} ({low_month['review_count']:,} reviews, {low_month['percentage']:.1f}%)")
    
    # Calculate seasonality factor
    peak_to_low_ratio = peak_month['review_count'] / low_month['review_count']
    print(f"📊 Seasonality factor: {peak_to_low_ratio:.1f}x more reviews in peak month vs low month")

# Bonus: Compare Ice Cream vs Soup seasonality
print("\n" + "="*60)
print("Bonus: Ice Cream vs Soup Seasonality Comparison")
print("="*60)

# Get Ice Cream reviews
ice_cream_businesses = business_df.filter(col("categories").contains("Ice Cream") | col("categories").contains("Frozen Yogurt"))
ice_cream_reviews = reviews_df.join(ice_cream_businesses.select("business_id"), "business_id") \
    .withColumn("review_month", month(to_date(col("date")))) \
    .groupBy("review_month") \
    .agg(count("*").alias("ice_cream_reviews"))

# Get Soup reviews
soup_businesses = business_df.filter(col("categories").contains("Soup") | col("categories").contains("Ramen"))
soup_reviews = reviews_df.join(soup_businesses.select("business_id"), "business_id") \
    .withColumn("review_month", month(to_date(col("date")))) \
    .groupBy("review_month") \
    .agg(count("*").alias("soup_reviews"))

# Combine for comparison
comparison = ice_cream_reviews.join(soup_reviews, "review_month", "full") \
    .orderBy("review_month")

print("\nMonthly Comparison: Ice Cream vs Soup")
print("-" * 70)
print(f"{'Month':<12} {'Ice Cream':>15} {'Soup':>15} {'Difference':>15}")
print("-" * 70)

for row in comparison.collect():
    month_name = months[row['review_month']]
    ice_cream = row['ice_cream_reviews'] if row['ice_cream_reviews'] else 0
    soup = row['soup_reviews'] if row['soup_reviews'] else 0
    diff = ice_cream - soup
    print(f"  {month_name:<12} {ice_cream:>12,} {soup:>12,} {diff:>+14,}")

# Find months where each peaks
ice_cream_peak = comparison.orderBy(desc("ice_cream_reviews")).first()
soup_peak = comparison.orderBy(desc("soup_reviews")).first()

print(f"\n🍦 Ice Cream peak month: {months[ice_cream_peak['review_month']]}")
print(f"🥣 Soup peak month: {months[soup_peak['review_month']]}")
print(f"\n💡 Insight: Ice Cream is more popular in summer months, Soup in winter months")