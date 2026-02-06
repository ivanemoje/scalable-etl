from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryTables") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest-scalable:8181") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio-scalable:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", "demo") \
    .getOrCreate()

print("\n" + "="*80)
print("SILVER LAYER: warehouse.silver_listens")
print("="*80)
spark.sql("SELECT * FROM warehouse.silver_listens").show(truncate=False)

print("\n" + "="*80)
print("GOLD LAYER: warehouse.gold.user_peaks")
print("="*80)
spark.sql("SELECT * FROM warehouse.gold.user_peaks").show(truncate=False)

print("\n" + "="*80)
print("TABLE METADATA")
print("="*80)
print("\nSilver Layer Schema:")
spark.sql("DESCRIBE warehouse.silver_listens").show(truncate=False)

print("\nGold Layer Schema:")
spark.sql("DESCRIBE warehouse.gold.user_peaks").show(truncate=False)

spark.stop()