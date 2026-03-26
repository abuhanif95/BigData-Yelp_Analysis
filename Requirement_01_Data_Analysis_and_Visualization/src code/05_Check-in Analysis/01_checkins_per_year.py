%pyspark
# Task V.1: Count the number of check-ins per year
print("="*60)
print("Task V.1: Check-ins Per Year Analysis")
print("="*60)

from pyspark.sql.functions import explode, split, year, to_date, count, col

# Parse check-in dates (they are comma-separated strings)
# First explode each check-in date string into individual dates
checkins_exploded = checkins_df.select(
    "business_id",
    explode(split(col("date"), ", ")).alias("checkin_datetime")
)

# Extract year from each check-in datetime
checkins_with_year = checkins_exploded.withColumn(
    "checkin_year", 
    year(to_date(col("checkin_datetime")))
)

# Count check-ins per year
checkins_per_year = checkins_with_year.groupBy("checkin_year") \
    .agg(count("*").alias("checkin_count")) \
    .orderBy("checkin_year")

# Display results
checkins_per_year.createOrReplaceTempView("checkins_per_year")
z.show(checkins_per_year)

# Print summary
print("\nCheck-ins Per Year:")
print("-" * 40)
total_checkins = 0
for row in checkins_per_year.collect():
    year_val = row['checkin_year']
    count_val = row['checkin_count']
    total_checkins += count_val
    print(f"  {year_val}: {count_val:>12,} check-ins")

print("-" * 40)
print(f"  Total: {total_checkins:>12,} check-ins")

# Find year with most check-ins
most_checkins_year = checkins_per_year.orderBy(desc("checkin_count")).first()
print(f"\n📈 Year with most check-ins: {most_checkins_year['checkin_year']} ({most_checkins_year['checkin_count']:,} check-ins)")