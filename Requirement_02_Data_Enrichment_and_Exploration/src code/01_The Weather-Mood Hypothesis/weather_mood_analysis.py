%pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("="*100)
print("WEATHER-MOOD HYPOTHESIS ANALYSIS")
print("="*100)

# ============================================================================
# LOAD AND PREPARE DATA
# ============================================================================

print("\n[Step 1] Loading and preparing data...")

# Create standardized views with correct column names
spark.sql("""
CREATE OR REPLACE TEMP VIEW reviews_std AS
SELECT 
    review_id,
    rev_user_id AS user_id,
    rev_business_id AS business_id,
    rev_stars AS stars,
    rev_useful AS useful,
    rev_funny AS funny,
    rev_cool AS cool,
    rev_date AS date,
    rev_text AS text
FROM review
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW business_std AS
SELECT * FROM business
""")

# Verify data
review_count = spark.sql("SELECT COUNT(*) FROM reviews_std").collect()[0][0]
business_count = spark.sql("SELECT COUNT(*) FROM business_std").collect()[0][0]
print(f"Reviews: {review_count:,}")
print(f"Businesses: {business_count:,}")

# ============================================================================
# ANALYSIS 1: AVERAGE RATING BY WEATHER CONDITION
# ============================================================================

print("\n" + "="*100)
print("Analysis 1: Average Rating by Weather Condition")
print("Do bad weather conditions cause lower ratings?")
print("="*100)

# Define weather categories based on actual weather data
weather_analysis = spark.sql("""
WITH reviews_with_weather AS (
    SELECT 
        r.stars,
        w.temp_avg_f,
        w.precipitation_in,
        w.wind_speed
    FROM reviews_std r
    JOIN weather_data w ON to_date(r.date) = w.weather_date
    WHERE r.date IS NOT NULL 
      AND w.weather_date IS NOT NULL
),
weather_categorized AS (
    SELECT 
        stars,
        CASE 
            -- Wind conditions
            WHEN wind_speed >= 30 THEN 'Strong Wind'
            -- Temperature extremes
            WHEN temp_avg_f >= 95 THEN 'Extreme Heat'
            WHEN temp_avg_f <= 20 THEN 'Extreme Cold'
            -- Snow/rain conditions
            WHEN precipitation_in >= 0.5 AND temp_avg_f <= 32 THEN 'Heavy Snow'
            WHEN precipitation_in >= 0.1 AND precipitation_in < 0.5 AND temp_avg_f <= 32 THEN 'Snow'
            WHEN precipitation_in >= 0.5 THEN 'Heavy Rain'
            WHEN precipitation_in > 0 THEN 'Rain'
            ELSE 'Normal'
        END as weather_condition
    FROM reviews_with_weather
)
SELECT 
    weather_condition,
    COUNT(*) as total_reviews,
    ROUND(AVG(stars), 3) as avg_rating,
    SUM(CASE WHEN stars = 1 THEN 1 ELSE 0 END) as one_star_count,
    ROUND((SUM(CASE WHEN stars = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as one_star_pct,
    SUM(CASE WHEN stars = 5 THEN 1 ELSE 0 END) as five_star_count,
    ROUND((SUM(CASE WHEN stars = 5 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as five_star_pct
FROM weather_categorized
GROUP BY weather_condition
ORDER BY avg_rating ASC
""")

# Display the results in the format you want
print("\n" + "-"*100)
weather_analysis.show(truncate=False)
print("-"*100)

# ============================================================================
# ANALYSIS 1B: DETAILED BREAKDOWN BY WEATHER CONDITION
# ============================================================================

print("\n" + "="*100)
print("Analysis 1B: Detailed Weather Impact Breakdown")
print("="*100)

# Get more detailed statistics
detailed_weather = spark.sql("""
WITH reviews_with_weather AS (
    SELECT 
        r.stars,
        w.temp_avg_f,
        w.precipitation_in,
        w.wind_speed
    FROM reviews_std r
    JOIN weather_data w ON to_date(r.date) = w.weather_date
    WHERE r.date IS NOT NULL 
      AND w.weather_date IS NOT NULL
),
weather_categorized AS (
    SELECT 
        stars,
        CASE 
            WHEN wind_speed >= 30 THEN 'Strong Wind'
            WHEN temp_avg_f >= 95 THEN 'Extreme Heat'
            WHEN temp_avg_f <= 20 THEN 'Extreme Cold'
            WHEN precipitation_in >= 0.5 AND temp_avg_f <= 32 THEN 'Heavy Snow'
            WHEN precipitation_in >= 0.1 AND precipitation_in < 0.5 AND temp_avg_f <= 32 THEN 'Snow'
            WHEN precipitation_in >= 0.5 THEN 'Heavy Rain'
            WHEN precipitation_in > 0 THEN 'Rain'
            ELSE 'Normal'
        END as weather_condition,
        temp_avg_f,
        precipitation_in,
        wind_speed
    FROM reviews_with_weather
)
SELECT 
    weather_condition,
    COUNT(*) as total_reviews,
    ROUND(AVG(stars), 3) as avg_rating,
    ROUND(MIN(stars), 1) as min_rating,
    ROUND(MAX(stars), 1) as max_rating,
    ROUND(STDDEV(stars), 3) as rating_stddev,
    ROUND(AVG(temp_avg_f), 1) as avg_temp_f,
    ROUND(AVG(precipitation_in), 2) as avg_precip_in,
    ROUND(AVG(wind_speed), 1) as avg_wind_speed
FROM weather_categorized
GROUP BY weather_condition
ORDER BY avg_rating DESC
""")

print("\nDetailed Weather Impact Statistics:")
detailed_weather.show(truncate=False)

# ============================================================================
# ANALYSIS 2: RATING DISTRIBUTION BY WEATHER
# ============================================================================

print("\n" + "="*100)
print("Analysis 2: Rating Distribution by Weather Condition")
print("="*100)

rating_distribution = spark.sql("""
WITH reviews_with_weather AS (
    SELECT 
        r.stars,
        CASE 
            WHEN w.wind_speed >= 30 THEN 'Strong Wind'
            WHEN w.temp_avg_f >= 95 THEN 'Extreme Heat'
            WHEN w.temp_avg_f <= 20 THEN 'Extreme Cold'
            WHEN w.precipitation_in >= 0.5 AND w.temp_avg_f <= 32 THEN 'Heavy Snow'
            WHEN w.precipitation_in >= 0.1 AND w.precipitation_in < 0.5 AND w.temp_avg_f <= 32 THEN 'Snow'
            WHEN w.precipitation_in >= 0.5 THEN 'Heavy Rain'
            WHEN w.precipitation_in > 0 THEN 'Rain'
            ELSE 'Normal'
        END as weather_condition
    FROM reviews_std r
    JOIN weather_data w ON to_date(r.date) = w.weather_date
    WHERE r.date IS NOT NULL 
)
SELECT 
    weather_condition,
    SUM(CASE WHEN stars = 1 THEN 1 ELSE 0 END) as star_1,
    SUM(CASE WHEN stars = 2 THEN 1 ELSE 0 END) as star_2,
    SUM(CASE WHEN stars = 3 THEN 1 ELSE 0 END) as star_3,
    SUM(CASE WHEN stars = 4 THEN 1 ELSE 0 END) as star_4,
    SUM(CASE WHEN stars = 5 THEN 1 ELSE 0 END) as star_5,
    COUNT(*) as total
FROM reviews_with_weather
GROUP BY weather_condition
ORDER BY weather_condition
""")

print("\nRating Distribution by Weather Condition:")
rating_distribution.show(truncate=False)

# ============================================================================
# ANALYSIS 3: 1-STAR REVIEW SPIKES BY WEATHER
# ============================================================================

print("\n" + "="*100)
print("Analysis 3: 1-Star Review Spikes by Weather Condition")
print("="*100)

one_star_analysis = spark.sql("""
WITH reviews_with_weather AS (
    SELECT 
        r.stars,
        CASE 
            WHEN w.wind_speed >= 30 THEN 'Strong Wind'
            WHEN w.temp_avg_f >= 95 THEN 'Extreme Heat'
            WHEN w.temp_avg_f <= 20 THEN 'Extreme Cold'
            WHEN w.precipitation_in >= 0.5 AND w.temp_avg_f <= 32 THEN 'Heavy Snow'
            WHEN w.precipitation_in >= 0.1 AND w.precipitation_in < 0.5 AND w.temp_avg_f <= 32 THEN 'Snow'
            WHEN w.precipitation_in >= 0.5 THEN 'Heavy Rain'
            WHEN w.precipitation_in > 0 THEN 'Rain'
            ELSE 'Normal'
        END as weather_condition
    FROM reviews_std r
    JOIN weather_data w ON to_date(r.date) = w.weather_date
),
weather_stats AS (
    SELECT 
        weather_condition,
        COUNT(*) as total_reviews,
        SUM(CASE WHEN stars = 1 THEN 1 ELSE 0 END) as one_star_count,
        ROUND((SUM(CASE WHEN stars = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as one_star_pct
    FROM reviews_with_weather
    GROUP BY weather_condition
)
SELECT 
    weather_condition,
    total_reviews,
    one_star_count,
    one_star_pct,
    ROUND(one_star_pct - (SELECT AVG(one_star_pct) FROM weather_stats), 2) as deviation_from_avg
FROM weather_stats
ORDER BY one_star_pct ASC
""")

print("\n1-Star Review Percentage by Weather Condition:")
one_star_analysis.show(truncate=False)

# ============================================================================
# ANALYSIS 4: FIVE-STAR REVIEWS BY WEATHER
# ============================================================================

print("\n" + "="*100)
print("Analysis 4: Five-Star Review Patterns by Weather")
print("="*100)

five_star_analysis = spark.sql("""
WITH reviews_with_weather AS (
    SELECT 
        r.stars,
        CASE 
            WHEN w.wind_speed >= 30 THEN 'Strong Wind'
            WHEN w.temp_avg_f >= 95 THEN 'Extreme Heat'
            WHEN w.temp_avg_f <= 20 THEN 'Extreme Cold'
            WHEN w.precipitation_in >= 0.5 AND w.temp_avg_f <= 32 THEN 'Heavy Snow'
            WHEN w.precipitation_in >= 0.1 AND w.precipitation_in < 0.5 AND w.temp_avg_f <= 32 THEN 'Snow'
            WHEN w.precipitation_in >= 0.5 THEN 'Heavy Rain'
            WHEN w.precipitation_in > 0 THEN 'Rain'
            ELSE 'Normal'
        END as weather_condition
    FROM reviews_std r
    JOIN weather_data w ON to_date(r.date) = w.weather_date
)
SELECT 
    weather_condition,
    COUNT(*) as total_reviews,
    SUM(CASE WHEN stars = 5 THEN 1 ELSE 0 END) as five_star_count,
    ROUND((SUM(CASE WHEN stars = 5 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as five_star_pct
FROM reviews_with_weather
GROUP BY weather_condition
ORDER BY five_star_pct DESC
""")

print("\nFive-Star Review Percentage by Weather Condition:")
five_star_analysis.show(truncate=False)

# ============================================================================
# ANALYSIS 5: WEATHER IMPACT ON CHECK-INS (if available)
# ============================================================================

print("\n" + "="*100)
print("Analysis 5: Check-in Volume by Weather Condition")
print("="*100)

try:
    checkin_analysis = spark.sql("""
    WITH checkin_with_weather AS (
        SELECT 
            c.business_id,
            c.checkin_dates,
            w.weather_date,
            w.temp_avg_f,
            w.precipitation_in,
            w.wind_speed
        FROM checkin c
        LATERAL VIEW EXPLODE(SPLIT(c.checkin_dates, ', ')) t AS checkin_date
        JOIN weather_data w ON to_date(checkin_date) = w.weather_date
        WHERE checkin_date IS NOT NULL AND checkin_date != ''
    ),
    checkin_categorized AS (
        SELECT 
            CASE 
                WHEN wind_speed >= 30 THEN 'Strong Wind'
                WHEN temp_avg_f >= 95 THEN 'Extreme Heat'
                WHEN temp_avg_f <= 20 THEN 'Extreme Cold'
                WHEN precipitation_in >= 0.5 AND temp_avg_f <= 32 THEN 'Heavy Snow'
                WHEN precipitation_in >= 0.1 AND precipitation_in < 0.5 AND temp_avg_f <= 32 THEN 'Snow'
                WHEN precipitation_in >= 0.5 THEN 'Heavy Rain'
                WHEN precipitation_in > 0 THEN 'Rain'
                ELSE 'Normal'
            END as weather_condition,
            1 as checkin_count
        FROM checkin_with_weather
    )
    SELECT 
        weather_condition,
        COUNT(*) as total_checkins,
        ROUND(COUNT(*) / (SELECT COUNT(DISTINCT weather_date) FROM weather_data), 0) as avg_daily_checkins
    FROM checkin_categorized
    GROUP BY weather_condition
    ORDER BY avg_daily_checkins DESC
    """)
    
    print("\nCheck-in Volume by Weather Condition:")
    checkin_analysis.show(truncate=False)
    
except Exception as e:
    print(f"Check-in analysis not available: {e}")

# ============================================================================
# CREATE FINAL SUMMARY TABLE
# ============================================================================

print("\n" + "="*100)
print("FINAL SUMMARY - Weather Impact on Yelp Reviews")
print("="*100)

# Save the main analysis results
weather_analysis.write.mode("overwrite").saveAsTable("weather_rating_analysis")

# Create formatted output
final_summary = spark.sql("""
SELECT 
    weather_condition,
    total_reviews,
    avg_rating,
    one_star_count,
    CONCAT(one_star_pct, '%') as one_star_pct,
    five_star_count,
    CONCAT(five_star_pct, '%') as five_star_pct
FROM weather_rating_analysis
ORDER BY avg_rating ASC
""")

print("\n=== Analysis 1: Average Rating by Weather Condition ===")
print("Do bad weather conditions cause lower ratings?")
print("-"*100)
final_summary.show(truncate=False)
print("-"*100)

# ============================================================================
# KEY INSIGHTS
# ============================================================================

print("\n" + "="*100)
print("KEY INSIGHTS FROM ANALYSIS")
print("="*100)

# Get statistics from the analysis
results = weather_analysis.collect()

print("\n📊 Weather Impact Summary:")
print("-"*60)
for row in results:
    condition = row['weather_condition']
    avg_rating = row['avg_rating']
    one_star = row['one_star_pct']
    five_star = row['five_star_pct']
    total = row['total_reviews']
    
    print(f"\n{condition}:")
    print(f"  • Average Rating: {avg_rating:.3f} stars")
    print(f"  • 1-Star Reviews: {one_star:.2f}% ({row['one_star_count']:,} reviews)")
    print(f"  • 5-Star Reviews: {five_star:.2f}% ({row['five_star_count']:,} reviews)")
    print(f"  • Total Reviews: {total:,}")

# Find the best and worst weather conditions
best_weather = weather_analysis.orderBy(desc("avg_rating")).first()
worst_weather = weather_analysis.orderBy("avg_rating").first()

print("\n" + "="*100)
print("KEY FINDINGS:")
print("="*100)
print(f"\n✅ BEST Weather for Reviews: {best_weather['weather_condition']}")
print(f"   → Average Rating: {best_weather['avg_rating']:.3f} stars")
print(f"   → 5-Star Rate: {best_weather['five_star_pct']:.2f}%")
print(f"   → 1-Star Rate: {best_weather['one_star_pct']:.2f}%")

print(f"\n❌ WORST Weather for Reviews: {worst_weather['weather_condition']}")
print(f"   → Average Rating: {worst_weather['avg_rating']:.3f} stars")
print(f"   → 5-Star Rate: {worst_weather['five_star_pct']:.2f}%")
print(f"   → 1-Star Rate: {worst_weather['one_star_pct']:.2f}%")

print("\n" + "="*100)
print("Analysis Complete!")
print("="*100)