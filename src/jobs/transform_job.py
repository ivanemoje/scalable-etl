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
logger = logging.getLogger("SparkTransform")

def run_spark_transform():
    logger.info("Starting Spark Transformation Job...")
    
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
            .getOrCreate()

        bronze_path = "/home/iceberg/data/outputs/bronze_listens"
        
        if not os.path.exists(bronze_path):
            logger.error(f"Bronze path not found: {bronze_path}")
            sys.exit(1)

        df = spark.read.parquet(bronze_path)
        
        # Silver Layer
        window_spec = Window.partitionBy("user_name", "listened_at").orderBy(F.col("listened_at").desc())
        silver_df = df.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1).drop("rn") \
            .withColumn("listened_date", F.from_unixtime(F.col("listened_at")).cast("date")) \
            .coalesce(1) # Reduce file count to prevent timeouts

        # Gold Layer
        daily_counts = silver_df.groupBy("user_name", "listened_date").count()
        gold_window = Window.partitionBy("user_name").orderBy(F.col("count").desc())
        gold_df = daily_counts.withColumn("rank", F.row_number().over(gold_window)) \
            .filter(F.col("rank") <= 3).drop("rank") \
            .coalesce(1)

        # Persistence with Explicit Clean-up
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")
        
        logger.info("Saving Silver...")
        spark.sql("DROP TABLE IF EXISTS demo.silver_listens")
        silver_df.writeTo("demo.silver_listens").create()
        
        logger.info("Saving Gold...")
        spark.sql("DROP TABLE IF EXISTS demo.gold.user_peaks")
        gold_df.writeTo("demo.gold.user_peaks").create()
        
        logger.info("-> Success!")

    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals(): spark.stop()

if __name__ == "__main__":
    run_spark_transform()