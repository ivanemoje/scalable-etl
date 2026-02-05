import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Setup Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("GlueIcebergTransform")

def run_glue_transform():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_S3_PATH', 'CATALOG_NAME'])
    job.init(args['JOB_NAME'], args)
    
    bronze_path = args['BRONZE_S3_PATH']
    catalog = args['CATALOG_NAME']

    try:
        logger.info(f"Reading bronze data from: {bronze_path}")
        df = spark.read.parquet(bronze_path)

        # Silver Logic: Deduplication
        window_spec = Window.partitionBy("user_name", "listened_at").orderBy(F.col("listened_at").desc())
        silver_df = df.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1).drop("rn") \
            .withColumn("listened_date", F.from_unixtime(F.col("listened_at")).cast("date"))

        # Gold Logic: Top 3 days per user
        daily_counts = silver_df.groupBy("user_name", "listened_date").count()
        gold_window = Window.partitionBy("user_name").orderBy(F.col("count").desc())
        
        gold_df = daily_counts.withColumn("rank", F.row_number().over(gold_window)) \
            .filter(F.col("rank") <= 3).drop("rank")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold")
        
        gold_df.writeTo(f"{catalog}.gold.user_peaks") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        logger.info("-> Gold (Iceberg/Glue) transformation complete.")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise e
    finally:
        job.commit()

if __name__ == "__main__":
    run_glue_transform()