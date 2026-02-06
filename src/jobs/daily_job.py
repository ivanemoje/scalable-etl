import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DailyJob")

def run_daily_job():
    logger.info("Starting Daily Aggregation Job...")
    
    try:
        spark = SparkSession.builder \
            .appName("DailyJob") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "rest") \
            .config("spark.sql.catalog.demo.uri", "http://iceberg-rest-scalable:8181") \
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse") \
            .config("spark.sql.catalog.demo.s3.endpoint", "http://minio-scalable:9000") \
            .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
            .config("spark.sql.defaultCatalog", "demo") \
            .getOrCreate()

        # Read from Silver layer in Iceberg
        logger.info("Reading from Silver layer...")
        silver_df = spark.table("warehouse.silver_listens")
        
        # Gold Layer: Daily Top 3 counts per user
        logger.info("Computing daily aggregations...")
        daily_counts = silver_df.groupBy("user_name", "listened_date").count()
        gold_window = Window.partitionBy("user_name").orderBy(F.col("count").desc())
        gold_df = daily_counts.withColumn("rank", F.row_number().over(gold_window)) \
            .filter(F.col("rank") <= 3).drop("rank") \
            .coalesce(1)

        # Overwrite Gold layer
        logger.info("Saving to Gold layer...")
        spark.sql("DROP TABLE IF EXISTS warehouse.gold.user_peaks")
        gold_df.writeTo("warehouse.gold.user_peaks").create()
        
        logger.info("-> Daily Job Successful!")

    except Exception as e:
        logger.error(f"Daily Job Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals(): 
            spark.stop()

if __name__ == "__main__":
    run_daily_job()