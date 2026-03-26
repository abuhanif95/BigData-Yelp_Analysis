%pyspark
# Task V.2: Count the number of check-ins per hour within a 24-hour period
print("="*60)
print("Task V.2: Check-ins Per Hour Analysis")
print("="*60)

from pyspark.sql.functions import hour, to_timestamp

# Extract hour from each check-in datetime
checkins_with_hour = checkins_exploded.withColumn(
    "checkin_hour", 
    hour(to_timestamp(col("checkin_datetime"), "yyyy-MM-dd HH:mm:ss"))
)

# Count check-ins per hour
checkins_per_hour = checkins_with_hour.groupBy("checkin_hour") \
    .agg(count("*").alias("checkin_count")) \
    .orderBy("checkin_hour")

# Display results
checkins_per_hour.createOrReplaceTempView("checkins_per_hour")
z.show(checkins_per_hour)

# Print summary
print("\nCheck-ins Per Hour:")
print("-" * 50)
print(f"{'Hour':<10} {'Check-ins':>15} {'Bar Chart':>20}")
print("-" * 50)

for row in checkins_per_hour.collect():
    hour_val = row['checkin_hour']
    count_val = row['checkin_count']
    bar_length = int(count_val / checkins_per_hour.agg(max("checkin_count")).collect()[0][0] * 40)
    bar = "█" * bar_length
    print(f"  {hour_val:02d}:00 - {hour_val:02d}:59   {count_val:>12,}   {bar}")

# Find peak hours
peak_hours = checkins_per_hour.orderBy(desc("checkin_count")).limit(3)
print(f"\n🏆 Peak check-in hours:")
for row in peak_hours.collect():
    print(f"  {row['checkin_hour']:02d}:00 - {row['checkin_hour']:02d}:59: {row['checkin_count']:,} check-ins")

# Find off-peak hours
off_peak_hours = checkins_per_hour.orderBy("checkin_count").limit(3)
print(f"\n🌙 Off-peak check-in hours:")
for row in off_peak_hours.collect():
    print(f"  {row['checkin_hour']:02d}:00 - {row['checkin_hour']:02d}:59: {row['checkin_count']:,} check-ins")