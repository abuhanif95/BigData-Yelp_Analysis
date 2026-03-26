# Task V.5: Month-over-Month check-in growth rate for top 50 restaurants in a specific city
print("="*60)
print("Task V.5: Month-over-Month Check-in Growth Analysis")
print("="*60)

from pyspark.sql.functions import month, year, lag, col, count, when, round, desc
from pyspark.sql.window import Window

# First, let's find the most popular city from previous analysis
# If checkins_per_city is not available, calculate it
try:
    most_popular_city = checkins_per_city.first()
    city_to_analyze = most_popular_city['city']
    state_to_analyze = most_popular_city['state']
    print(f"\nAnalyzing city: {city_to_analyze}, {state_to_analyze}")
except:
    # If checkins_per_city not available, calculate it now
    print("\nCalculating most popular city...")
    
    # Parse check-in dates
    checkins_exploded_local = checkins_df.select(
        "business_id",
        explode(split(col("date"), ", ")).alias("checkin_datetime")
    )
    
    # Join with business data to get city
    checkins_with_city_local = checkins_exploded_local.join(
        business_df.select("business_id", "city", "state"), 
        "business_id"
    )
    
    # Count check-ins per city
    checkins_per_city_local = checkins_with_city_local.groupBy("city", "state") \
        .agg(count("*").alias("checkin_count")) \
        .orderBy(desc("checkin_count"))
    
    most_popular_city = checkins_per_city_local.first()
    city_to_analyze = most_popular_city['city']
    state_to_analyze = most_popular_city['state']
    print(f"✓ Most popular city: {city_to_analyze}, {state_to_analyze}")

# Parse check-in dates if not already done
try:
    checkins_exploded.count()
except:
    checkins_exploded = checkins_df.select(
        "business_id",
        explode(split(col("date"), ", ")).alias("checkin_datetime")
    )

# Get top 50 businesses in this city by check-ins
print(f"\nIdentifying top 50 businesses in {city_to_analyze}...")

# First, get all check-ins for businesses in this city
checkins_in_city = checkins_exploded.join(
    business_df.filter((col("city") == city_to_analyze) & (col("state") == state_to_analyze)),
    "business_id"
)

# Count check-ins per business
checkins_per_business_city = checkins_in_city.groupBy("business_id") \
    .agg(count("*").alias("checkin_count"))

# Get top 50 businesses
top_businesses_in_city = checkins_per_business_city \
    .orderBy(desc("checkin_count")) \
    .limit(50)

top_business_ids = [row['business_id'] for row in top_businesses_in_city.collect()]
print(f"✓ Top {len(top_business_ids)} businesses identified in {city_to_analyze}")

# Get check-ins for these businesses with month and year
print("\nCalculating monthly check-in trends...")
checkins_for_top = checkins_exploded.filter(col("business_id").isin(top_business_ids)) \
    .withColumn("checkin_year", year(to_date(col("checkin_datetime")))) \
    .withColumn("checkin_month", month(to_date(col("checkin_datetime"))))

# Count check-ins per month
monthly_checkins = checkins_for_top.groupBy("checkin_year", "checkin_month") \
    .agg(count("*").alias("checkin_count")) \
    .orderBy("checkin_year", "checkin_month")

# Calculate month-over-month growth
window_spec = Window.orderBy("checkin_year", "checkin_month")
monthly_checkins_with_growth = monthly_checkins.withColumn(
    "previous_month_count", 
    lag("checkin_count").over(window_spec)
).withColumn(
    "mom_growth", 
    when(col("previous_month_count").isNull(), 0)
    .otherwise(((col("checkin_count") - col("previous_month_count")) / col("previous_month_count")) * 100)
).withColumn(
    "mom_growth", 
    round("mom_growth", 2)
)

# Display results
monthly_checkins_with_growth.createOrReplaceTempView("monthly_checkins_growth")
z.show(monthly_checkins_with_growth)

# Print summary
print(f"\n" + "="*60)
print(f"Month-over-Month Check-in Growth for Top 50 Businesses in {city_to_analyze}:")
print("="*60)
print("-" * 80)
print(f"{'Year-Month':<12} {'Check-ins':>12} {'Previous':>12} {'Growth %':>12} {'Trend':>15}")
print("-" * 80)

trending_months = []
declining_months = []

for row in monthly_checkins_with_growth.collect():
    year_month = f"{row['checkin_year']}-{row['checkin_month']:02d}"
    checkins = row['checkin_count']
    previous = row['previous_month_count'] if row['previous_month_count'] else 0
    growth = row['mom_growth']
    
    if growth > 20:
        trend = "🚀 SURGING"
        trending_months.append((year_month, growth))
    elif growth > 0:
        trend = "📈 Growing"
    elif growth < -20:
        trend = "⚠️ DROPPING"
        declining_months.append((year_month, growth))
    elif growth < 0:
        trend = "📉 Declining"
    else:
        trend = "➡️ Stable"
    
    print(f"{year_month:<12} {checkins:>12,} {previous:>12,} {growth:>11.1f}% {trend:>15}")

# Summary statistics
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)

# Calculate average growth
avg_growth = monthly_checkins_with_growth.filter(col("previous_month_count").isNotNull()) \
    .select(round(avg("mom_growth"), 2).alias("avg_growth")).collect()[0][0]
print(f"  Average month-over-month growth: {avg_growth:+.2f}%")

# Identify trending periods
if trending_months:
    print(f"\n  🚀 Trending Periods (Growth > 20%):")
    for month_name, growth in trending_months[:5]:
        print(f"     - {month_name}: {growth:.1f}% growth")

if declining_months:
    print(f"\n  ⚠️ Declining Periods (Drop > 20%):")
    for month_name, growth in declining_months[:5]:
        print(f"     - {month_name}: {growth:.1f}% drop")

# Find best growth month
best_growth = monthly_checkins_with_growth.filter(col("previous_month_count").isNotNull()) \
    .orderBy(desc("mom_growth")).first()
if best_growth and best_growth['mom_growth'] > 0:
    print(f"\n  🏆 Best growth month: {best_growth['checkin_year']}-{best_growth['checkin_month']:02d}")
    print(f"     Growth: {best_growth['mom_growth']:.1f}%")
    print(f"     Check-ins: {best_growth['checkin_count']:,}")

# Find worst decline
worst_decline = monthly_checkins_with_growth.filter(col("previous_month_count").isNotNull()) \
    .orderBy("mom_growth").first()
if worst_decline and worst_decline['mom_growth'] < 0:
    print(f"\n  📉 Worst decline: {worst_decline['checkin_year']}-{worst_decline['checkin_month']:02d}")
    print(f"     Decline: {worst_decline['mom_growth']:.1f}%")
    print(f"     Check-ins: {worst_decline['checkin_count']:,}")

# Current trend (last 3 months)
print("\n" + "="*60)
print("RECENT TRENDS (Last 3 Months)")
print("="*60)

recent_months = monthly_checkins_with_growth.filter(col("previous_month_count").isNotNull()) \
    .orderBy(desc("checkin_year"), desc("checkin_month")) \
    .limit(3)

for row in recent_months.collect():
    year_month = f"{row['checkin_year']}-{row['checkin_month']:02d}"
    growth = row['mom_growth']
    trend_icon = "🔼" if growth > 0 else "🔽" if growth < 0 else "➡️"
    print(f"  {year_month}: {trend_icon} {growth:+.1f}% change")

# Identify currently trending businesses
print("\n" + "="*60)
print("CURRENTLY TRENDING BUSINESSES (High recent check-in volume)")
print("="*60)

# Get most recent month data
latest_month = monthly_checkins_with_growth.orderBy(desc("checkin_year"), desc("checkin_month")).first()
if latest_month:
    latest_year = latest_month['checkin_year']
    latest_month_num = latest_month['checkin_month']
    
    # Get businesses with highest check-ins in latest month
    recent_businesses = checkins_for_top.filter(
        (col("checkin_year") == latest_year) & 
        (col("checkin_month") == latest_month_num)
    ).groupBy("business_id") \
     .agg(count("*").alias("recent_checkins")) \
     .join(business_df.select("business_id", "name", "city", "stars", "review_count"), "business_id") \
     .orderBy(desc("recent_checkins")) \
     .limit(10)
    
    print(f"\n  Top trending businesses in {city_to_analyze} (Latest month: {latest_year}-{latest_month_num:02d}):")
    print("-" * 70)
    for i, row in enumerate(recent_businesses.collect(), 1):
        print(f"  {i:2}. {row['name'][:40]:40} | {row['recent_checkins']:>6} check-ins | {row['stars']:.1f}⭐")