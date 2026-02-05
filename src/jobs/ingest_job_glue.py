import sys
from awsglue.transforms import *
from awsglue.utils import get_arg_params
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def run_daily_queries():
    # Glue typically passes arguments via --GOLD_S3_PATH
    args = get_arg_params(sys.argv, ['GOLD_S3_PATH'])
    gold_s3_path = args['GOLD_S3_PATH']

    # Load S3 Parquet directly into Spark (No local sync needed)
    gold_peaks = spark.read.parquet(gold_s3_path)
    gold_peaks.createOrReplaceTempView("gold_peaks")

    print("\n" + "="*50 + "\nREPORT: TOP 10 USERS\n" + "="*50)
    spark.sql("""
        SELECT user_name, COUNT(*) AS listen_count 
        FROM gold_peaks 
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).show()

    print("\n" + "="*50 + "\nREPORT: 7-DAY ACTIVE TREND\n" + "="*50)
    spark.sql("""
        WITH daily_users AS (
            SELECT CAST(listened_at AS DATE) as d, user_name FROM gold_peaks GROUP BY 1, 2
        ),
        active_counts AS (
            SELECT curr.d as date, COUNT(DISTINCT past.user_name) as active_users
            FROM daily_users curr
            LEFT JOIN daily_users past ON past.d BETWEEN (curr.d - INTERVAL 6 DAYS) AND curr.d
            GROUP BY curr.d
        )
        SELECT date, active_users FROM active_counts ORDER BY date DESC LIMIT 7
    """).show()

if __name__ == "__main__":
    run_daily_queries()
    job.commit()