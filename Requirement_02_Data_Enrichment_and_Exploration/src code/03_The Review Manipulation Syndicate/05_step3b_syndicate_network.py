%pyspark
from pyspark.sql import functions as F

business_df = spark.table("business")

print("Step 3b: Building co-review network to detect syndicates...")

ghost_user_biz = (
    ghost_accounts
    .filter(F.col("ghost_score") >= 3)
    .select("user_id", "business_id")
    .distinct()
)

user_pairs = (
    ghost_user_biz.alias("a")
    .join(
        ghost_user_biz.alias("b"),
        (F.col("a.business_id") == F.col("b.business_id")) &
        (F.col("a.user_id")    <  F.col("b.user_id"))
    )
    .select(
        F.col("a.user_id").alias("user_1"),
        F.col("b.user_id").alias("user_2"),
        F.col("a.business_id").alias("shared_business")
    )
)

# --- Lowered threshold from 3 to 1 ---
co_review_network = (
    user_pairs
    .groupBy("user_1", "user_2")
    .agg(F.count("shared_business").alias("shared_biz_count"))
    .filter(F.col("shared_biz_count") >= 1)
    .orderBy("shared_biz_count", ascending=False)
)

print(f"\nSyndicate user-pair connections found: {co_review_network.count()}")
print("\n=== Strongest Syndicate Pairs (Most Shared Businesses) ===")
co_review_network.show(30, truncate=False)
z.show(co_review_network.limit(30))

# --- Node centrality ---
user_centrality = (
    co_review_network
    .select(F.col("user_1").alias("user_id"), "shared_biz_count")
    .union(
        co_review_network.select(F.col("user_2").alias("user_id"), "shared_biz_count")
    )
    .groupBy("user_id")
    .agg(
        F.count("*").alias("syndicate_connections"),
        F.sum("shared_biz_count").alias("total_co_reviews")
    )
    .orderBy("syndicate_connections", ascending=False)
)

print("\n=== Most Connected Ghost Users (Network Hub Accounts) ===")
user_centrality.show(20, truncate=False)
z.show(user_centrality.limit(20))

# --- Businesses most targeted ---
ghost_targeting = (
    ghost_user_biz
    .groupBy("business_id")
    .agg(F.count("user_id").alias("ghost_account_count"))
    .join(business_df.select("business_id", "name", "city", "state"), "business_id")
    .select("name", "city", "state", "ghost_account_count")
    .orderBy("ghost_account_count", ascending=False)
)

print("\n=== Businesses Most Targeted by Ghost Accounts ===")
ghost_targeting.show(20, truncate=False)
z.show(ghost_targeting.limit(20))