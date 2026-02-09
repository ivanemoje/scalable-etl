import logging
import sys
import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SparkTransform")

def run_spark_transform():
    logger.info("Starting Spark Transformation Job (Shuffle-Free)...")
    
    try:
        spark = SparkSession.builder \
            .appName("SparkTransform") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "rest") \
            .config("spark.sql.catalog.demo.uri", "http://iceberg-rest-scalable:8181") \
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse") \
            .config("spark.sql.catalog.demo.s3.endpoint", "http://minio-scalable:9000") \
            .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
            .config("spark.sql.defaultCatalog", "demo") \
            .config("spark.sql.files.ignoreCorruptFiles", "true") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()

        bronze_path = "/home/iceberg/data/outputs/bronze_listens"
        
        if not os.path.exists(bronze_path):
            logger.error(f"Bronze path not found: {bronze_path}")
            sys.exit(1)

        # Clean up empty parquet files before reading
        logger.info("Cleaning empty parquet files...")
        subprocess.run(
            f"find {bronze_path} -name '*.parquet' -size 0 -delete",
            shell=True,
            check=False
        )

        # ===================================================================
        # BRONZE LAYER → ICEBERG (Raw data as-is, partitioned by user)
        # ===================================================================
        logger.info("Loading Bronze layer (raw data)...")
        bronze_df = spark.read.parquet(bronze_path)
        
        # Create namespaces
        spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.gold")
        
        logger.info("Saving Bronze Layer to Iceberg (raw, partitioned by user)...")
        spark.sql("DROP TABLE IF EXISTS warehouse.bronze_listens")
        
        bronze_df.writeTo("warehouse.bronze_listens") \
            .partitionedBy("user_name") \
            .create()
        
        bronze_count = bronze_df.count()
        logger.info(f"✓ Bronze layer: {bronze_count} raw records")

        # ===================================================================
        # SILVER LAYER (Cleaned, deduplicated, enriched - NO SHUFFLE)
        # ===================================================================
        logger.info("Transforming to Silver layer (partition-aware)...")
        
        # Window partitioned by user_name to match bronze partitioning
        window_spec = Window.partitionBy("user_name", "listened_at") \
            .orderBy(F.col("listened_at").desc())
        
        silver_df = bronze_df \
            .withColumn("listened_datetime", F.from_unixtime(F.col("listened_at")).cast("timestamp")) \
            .withColumn("listened_date", F.to_date(F.col("listened_datetime"))) \
            .withColumn("year", F.year(F.col("listened_datetime"))) \
            .withColumn("month", F.month(F.col("listened_datetime"))) \
            .withColumn("day", F.dayofmonth(F.col("listened_datetime"))) \
            .withColumn("hour", F.hour(F.col("listened_datetime"))) \
            .withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1) \
            .drop("rn") \
            .repartition("user_name")
        
        logger.info("Saving Silver Layer to Iceberg (partitioned by user)...")
        spark.sql("DROP TABLE IF EXISTS warehouse.silver_listens")
        
        silver_df.writeTo("warehouse.silver_listens") \
            .partitionedBy("user_name") \
            .create()
        
        silver_count = silver_df.count()
        logger.info(f"✓ Silver layer: {silver_count} deduplicated records")

        # ===================================================================
        # GOLD LAYER (Aggregated metrics - partition-local aggregation)
        # ===================================================================
        logger.info("Creating Gold layer (partition-local aggregations)...")
        
        # Aggregate within user partitions - NO SHUFFLE
        gold_df = silver_df.groupBy("user_name", "listened_date") \
            .agg(
                F.count("*").alias("listen_count"),
                F.countDistinct("track_name").alias("unique_tracks"),
                F.countDistinct("artist_name").alias("unique_artists")
            )
        
        # Top 3 per user
        gold_window = Window.partitionBy("user_name") \
            .orderBy(F.col("listen_count").desc())
        
        gold_df = gold_df \
            .withColumn("rank", F.row_number().over(gold_window)) \
            .filter(F.col("rank") <= 3) \
            .select("user_name", "listened_date", "listen_count", "unique_tracks", "unique_artists") \
            .repartition("user_name")
        
        logger.info("Saving Gold Layer to Iceberg (partitioned by user)...")
        spark.sql("DROP TABLE IF EXISTS warehouse.gold.user_peaks")
        
        gold_df.writeTo("warehouse.gold.user_peaks") \
            .partitionedBy("user_name") \
            .create()
        
        gold_count = gold_df.count()
        logger.info(f"✓ Gold layer: {gold_count} aggregated records")
        
        # ===================================================================
        # SUMMARY
        # ===================================================================
        logger.info("=" * 70)
        logger.info("TRANSFORMATION SUMMARY (SHUFFLE-FREE)")
        logger.info("=" * 70)
        logger.info(f"Bronze (raw):        {bronze_count:,} records")
        logger.info(f"Silver (cleaned):    {silver_count:,} records")
        logger.info(f"Gold (aggregated):   {gold_count:,} records")
        logger.info("All layers partitioned by user_name")
        logger.info("=" * 70)
        logger.info("-> Transformation Job Successful!")

    except Exception as e:
        logger.error(f"Transform Job Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals(): 
            spark.stop()

if __name__ == "__main__":
    run_spark_transform()