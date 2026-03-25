%pyspark
# Task 5: Count number of different categories
from pyspark.sql.functions import explode, split

# Explode categories array
categories_df = business_df.filter(col("categories").isNotNull()) \
    .select(explode(split(col("categories"), ", ")).alias("category")) \
    .distinct()

category_count = categories_df.count()
print(f"Total number of distinct categories: {category_count}")

# Show sample of categories
categories_df.createOrReplaceTempView("categories")
z.show(categories_df.limit(50))