import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from s3 import delete_if_exists

# create/get spark + glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession

# SQL extensions fix for Spark 2.4.x, see SPARK-25003
sc._jvm.io.delta.sql.DeltaSparkSessionExtension().apply(
    spark._jsparkSession.extensions()
)
spark = SparkSession(sc, spark._jsparkSession.cloneSession())

# resolve arguments
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET", "TABLE"],
)
s3_bucket = args["BUCKET"]
TABLE = args["TABLE"]

print(f"bucket: {s3_bucket}")
print(f"table : {TABLE}")

# folder within S3 for the delta table
s3_prefix = f"delta/{TABLE}"
s3_path = f"s3://{s3_bucket}/{s3_prefix}"

# cleanup
delete_if_exists(bucket=s3_bucket, prefix=s3_prefix)

s3_full_load_path = f"s3://{s3_bucket}/dms/full/{TABLE}/"

print("\nLoad parquet files into dataframe...")
df = spark.read.load(s3_full_load_path)
df.show(5)

print("\nWrite table to disk...")
df.write.format("delta").save(s3_path)

print("\nCreate delta table...")
spark.sql(f"CREATE TABLE {TABLE} USING DELTA LOCATION '{s3_path}'")

print("\nCreate manifest...")
df.registerTempTable(TABLE)
spark.sql(f"SHOW TABLES").show()
spark.sql(f"GENERATE symlink_format_manifest FOR TABLE delta.`{s3_path}`")
