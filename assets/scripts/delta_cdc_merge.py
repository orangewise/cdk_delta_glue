import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import max
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

print(f"bucket        : {s3_bucket}")
print(f"table         : {TABLE}")


# folder within S3 for the delta table
TABLE_CHANGES = f"{TABLE}_changes"
s3_prefix = f"delta/{TABLE}"
s3_prefix_changes = f"delta/{TABLE_CHANGES}"
s3_path = f"s3://{s3_bucket}/{s3_prefix}"
s3_path_changes = f"s3://{s3_bucket}/{s3_prefix_changes}"

delete_if_exists(bucket=s3_bucket, prefix=s3_prefix_changes)

# TODO: this should be recursive for all folders under TABLE
s3_cdc_load_path = f"s3://{s3_bucket}/dms/cdc/{TABLE}/2021/06/11/"

print("\nLoad parquet files with changes into dataframe...")
df = spark.read.load(s3_cdc_load_path)

print("\nWrite table to disk...")
df.write.format("delta").save(s3_path_changes)

print(f"\nCreate delta table {TABLE_CHANGES}...")
spark.sql(f"CREATE TABLE {TABLE_CHANGES} USING DELTA LOCATION '{s3_path_changes}'")


print(f"\nRead table {TABLE}...")
dt = DeltaTable.forPath(sparkSession=spark, path=s3_path)
dtDF = dt.toDF()
# register so spark sql knows about it
dtDF.registerTempTable(TABLE)

print(f"\nRead table {TABLE_CHANGES}...")
dtc = DeltaTable.forPath(sparkSession=spark, path=s3_path_changes)
dtcDF = dtc.toDF()
# register so spark sql knows about it
dtcDF.registerTempTable(TABLE_CHANGES)

print(f"Tables in db...")
spark.sql(f"SHOW TABLES").show()


print(f"Merge changes...")
# Find the latest change for each key based on the timestamp
# Note: For nested structs, max on struct is computed as
# max on first struct field, if equal fall back to second fields, and so on.
latestChangeForEachKey = (
    dtcDF.selectExpr(
        "id", "struct(TIMESTAMP, datetime, channel, value, Op) as otherCols"
    )
    .groupBy("id")
    .agg(max("otherCols").alias("latest"))
    .select("id", "latest.*")
)

dt.alias("target").merge(
    latestChangeForEachKey.alias("source"), "target.id = source.id"
).whenMatchedDelete(condition="source.Op = 'D'").whenMatchedUpdate(
    set={
        "datetime": "source.datetime",
        "channel": "source.channel",
        "value": "source.value",
    }
).whenNotMatchedInsert(
    values={
        "TIMESTAMP": "source.TIMESTAMP",
        "id": "source.id",
        "datetime": "source.datetime",
        "channel": "source.channel",
        "value": "source.value",
    }
).execute()

print("\nCreate manifest...")
spark.sql(f"GENERATE symlink_format_manifest FOR TABLE delta.`{s3_path}`")
