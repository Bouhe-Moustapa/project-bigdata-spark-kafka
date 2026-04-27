"""
Spark Structured Streaming — Soccer Match Analytics
=====================================================
Reads JSON match events from the Kafka topic 'football-matches',
computes real-time aggregations, and writes results to Parquet.

Aggregations implemented:
  1. Points Table  — wins / draws / losses / points per team
  2. Goals by Team — total goals scored and conceded per team

Streaming concepts used:
  • Watermarking  (10-minute late-data tolerance)
  • Tumbling windows (5-minute windows for windowed goal statistics)

Usage (from the Jupyter container terminal):
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1 \\
        --conf spark.pyspark.python=/usr/bin/python3.11 \\
        --conf spark.pyspark.driver.python=/opt/conda/bin/python \\
        --conf spark.sql.shuffle.partitions=4 \\
        --conf spark.driver.memory=1024m \\
        --conf spark.executor.memory=1024m \\
        /home/jovyan/work/mini-project-spark-football/src/stream_job.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "football-matches")
# Use /tmp for checkpoints (always writable inside the container)
CHECKPOINT_BASE = "checkpoints/football-streaming"
OUTPUT_BASE = "outputs/stream_results"

# Ensure output directories exist
os.makedirs(f"{OUTPUT_BASE}/points_table", exist_ok=True)
os.makedirs(f"{OUTPUT_BASE}/goal_statistics", exist_ok=True)
os.makedirs(f"{CHECKPOINT_BASE}/points_table", exist_ok=True)
os.makedirs(f"{CHECKPOINT_BASE}/goal_statistics", exist_ok=True)

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("football-stream-analytics")
    # .master("local[*]") # Uncomment if running locally without a standalone master
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  Football Streaming Analytics")
print(f"  Spark version : {spark.version}")
print(f"  Kafka broker  : {KAFKA_SERVERS}")
print(f"  Topic         : {TOPIC}")
print("=" * 60)

# ---------------------------------------------------------------------------
# JSON Schema for the incoming messages
# ---------------------------------------------------------------------------
match_schema = StructType([
    StructField("season", StringType(), True),
    StructField("date", StringType(), True),
    StructField("home_team", StringType(), True),
    StructField("away_team", StringType(), True),
    StructField("home_goals", IntegerType(), True),
    StructField("away_goals", IntegerType(), True),
    StructField("total_goals", IntegerType(), True),
    StructField("goal_difference", IntegerType(), True),
    StructField("result", StringType(), True),
    StructField("match_year", IntegerType(), True),
])

# ---------------------------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# ---------------------------------------------------------------------------
# Parse JSON and add event_time for watermarking
# ---------------------------------------------------------------------------
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_timestamp")
    .select(
        F.from_json(F.col("json_value"), match_schema).alias("data"),
        F.col("kafka_timestamp").alias("event_time"),
    )
    .select("data.*", "event_time")
    .withWatermark("event_time", "10 minutes")
)

# ---------------------------------------------------------------------------
# AGGREGATION 1 — Points Table (per team, windowed)
# ---------------------------------------------------------------------------
# Explode each match into TWO rows: one for the home team, one for the away
# team, so we can compute points from each team's perspective.

home_perspective = (
    parsed_stream
    .select(
        F.col("home_team").alias("team"),
        F.col("event_time"),
        F.when(F.col("result") == "home_win", 3)
         .when(F.col("result") == "draw", 1)
         .otherwise(0).alias("points"),
        F.when(F.col("result") == "home_win", 1).otherwise(0).alias("wins"),
        F.when(F.col("result") == "draw", 1).otherwise(0).alias("draws"),
        F.when(F.col("result") == "away_win", 1).otherwise(0).alias("losses"),
        F.col("home_goals").alias("goals_scored"),
        F.col("away_goals").alias("goals_conceded"),
    )
)

away_perspective = (
    parsed_stream
    .select(
        F.col("away_team").alias("team"),
        F.col("event_time"),
        F.when(F.col("result") == "away_win", 3)
         .when(F.col("result") == "draw", 1)
         .otherwise(0).alias("points"),
        F.when(F.col("result") == "away_win", 1).otherwise(0).alias("wins"),
        F.when(F.col("result") == "draw", 1).otherwise(0).alias("draws"),
        F.when(F.col("result") == "home_win", 1).otherwise(0).alias("losses"),
        F.col("away_goals").alias("goals_scored"),
        F.col("home_goals").alias("goals_conceded"),
    )
)

all_team_events = home_perspective.union(away_perspective)

points_table = (
    all_team_events
    .groupBy(
        F.window("event_time", "5 minutes"),
        "team",
    )
    .agg(
        F.sum("points").alias("total_points"),
        F.sum("wins").alias("total_wins"),
        F.sum("draws").alias("total_draws"),
        F.sum("losses").alias("total_losses"),
        F.sum("goals_scored").alias("total_goals_scored"),
        F.sum("goals_conceded").alias("total_goals_conceded"),
        F.count("*").alias("matches_played"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "team",
        "matches_played",
        "total_wins",
        "total_draws",
        "total_losses",
        "total_points",
        "total_goals_scored",
        "total_goals_conceded",
        (F.col("total_goals_scored") - F.col("total_goals_conceded")).alias("goal_diff"),
    )
)

# ---------------------------------------------------------------------------
# AGGREGATION 2 — Windowed Goal Statistics (high-scoring matches)
# ---------------------------------------------------------------------------
goal_stats = (
    parsed_stream
    .groupBy(
        F.window("event_time", "5 minutes"),
    )
    .agg(
        F.count("*").alias("total_matches"),
        F.sum("total_goals").alias("total_goals"),
        F.avg("total_goals").alias("avg_goals_per_match"),
        F.max("total_goals").alias("max_goals_in_match"),
        F.sum(
            F.when(F.col("total_goals") >= 4, 1).otherwise(0)
        ).alias("high_scoring_matches"),
        F.sum(
            F.when(F.col("result") == "home_win", 1).otherwise(0)
        ).alias("home_wins"),
        F.sum(
            F.when(F.col("result") == "away_win", 1).otherwise(0)
        ).alias("away_wins"),
        F.sum(
            F.when(F.col("result") == "draw", 1).otherwise(0)
        ).alias("draws"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "total_matches",
        "total_goals",
        "avg_goals_per_match",
        "max_goals_in_match",
        "high_scoring_matches",
        "home_wins",
        "away_wins",
        "draws",
    )
)

# ---------------------------------------------------------------------------
# Write streams to Parquet
# ---------------------------------------------------------------------------
print("\nStarting streaming queries …\n")

query_points = (
    points_table.writeStream
    .queryName("points_table")
    .format("parquet")
    .option("path", f"{OUTPUT_BASE}/points_table")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/points_table")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

query_goals = (
    goal_stats.writeStream
    .queryName("goal_statistics")
    .format("parquet")
    .option("path", f"{OUTPUT_BASE}/goal_statistics")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/goal_statistics")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

print("  ✓ Points Table stream started")
print(f"    → output : {OUTPUT_BASE}/points_table")
print(f"    → checkpoint : {CHECKPOINT_BASE}/points_table\n")

print("  ✓ Goal Statistics stream started")
print(f"    → output : {OUTPUT_BASE}/goal_statistics")
print(f"    → checkpoint : {CHECKPOINT_BASE}/goal_statistics\n")

print("Waiting for streaming queries (Ctrl+C to stop) …\n")

# Wait until any query terminates (or user interrupts)
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping streaming queries …")
    query_points.stop()
    query_goals.stop()
    print("Done.")
