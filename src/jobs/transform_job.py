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
            .config("spark.sql.catalog.demo.uri", "http://localhost:8182") \
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse") \
            .config("spark.sql.catalog.demo.s3.endpoint", "http://localhost:9100") \
            .getOrCreate()

        bronze_path = "data/outputs/bronze_listens"
        
        if not os.path.exists(bronze_path):
            logger.error(f"Bronze path not found: {os.path.abspath(bronze_path)}")
            sys.exit(1)

        logger.info(f"Reading bronze data from: {bronze_path}")
        df = spark.read.parquet(bronze_path)

        window_spec = Window.partitionBy("user_name", "listened_at").orderBy(F.col("listened_at").desc())
        
        silver_df = df.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1).drop("rn") \
            .withColumn("listened_date", F.from_unixtime(F.col("listened_at")).cast("date"))

        daily_counts = silver_df.groupBy("user_name", "listened_date").count()
        gold_window = Window.partitionBy("user_name").orderBy(F.col("count").desc())
        
        gold_df = daily_counts.withColumn("rank", F.row_number().over(gold_window)) \
            .filter(F.col("rank") <= 3).drop("rank")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")
        gold_df.writeTo("demo.gold.user_peaks").createOrReplace()
        
        logger.info("-> Gold (Iceberg/S3) transformation complete.")

    except Exception as e:
        logger.error(f"CRITICAL: Spark transformation failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    run_spark_transform()