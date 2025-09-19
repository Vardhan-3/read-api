import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

## ----------------- INIT -----------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## ----------------- CONFIG -----------------
BUCKET = "uk-property-bronze"

# CSV read options with error tolerance
CSV_OPTS = {
    "quoteChar": "\"",
    "withHeader": True,
    "separator": ",",
    "multiline": "true",
    "mode": "PERMISSIVE",               # tolerate bad rows
    "columnNameOfCorruptRecord": "_corrupt_record"
}

PARQUET_OPTS = {
    "compression": "snappy"
}

## ----------------- HELPERS -----------------
def drop_corrupt(dyf):
    """Remove rows that failed CSV parsing."""
    return Filter.apply(frame=dyf, f=lambda x: x["_corrupt_record"] is None)

def write_silver(dyf, path, partition_keys=None):
    """Clean write to silver layer."""
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": path, "partitionKeys": partition_keys or []},
        format_options=PARQUET_OPTS,
        transformation_ctx=f"silver_{path.split('/')[-2]}"
    )

## ----------------- 1. HMLR BRONZE → SILVER -----------------
hmlr_raw = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [f"{BUCKET}/bronze/hmlr/price-paid/yearly_parquet/"], "recurse": True},
    transformation_ctx="hmlr_raw"
)

hmlr_clean = hmlr_raw.select_fields(["transaction_id", "price", "date", "postcode", "prop_type", "year"]) \
                      .apply_mapping([
                          ("transaction_id", "string", "transaction_id", "string"),
                          ("price", "bigint", "price", "int"),
                          ("date", "string", "date", "date"),
                          ("postcode", "string", "postcode", "string"),
                          ("prop_type", "string", "prop_type", "string"),
                          ("year", "bigint", "year", "int")
                      ]) \
                      .dropDuplicates(["transaction_id"])

write_silver(hmlr_clean, f"{BUCKET}/silver/hmlr/", partition_keys=["year"])

## ----------------- 2. EPC ZIP BRONZE → SILVER -----------------
epc_raw = glueContext.create_dynamic_frame.from_options(
    format_options=CSV_OPTS,
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f"{BUCKET}/bronze/epc/domestic/epc-domestic-all-certificates.zip"], "recurse": True},
    transformation_ctx="epc_raw"
)

epc_clean = drop_corrupt(epc_raw) \
    .select_fields(["lmk-key", "address1", "address2", "address3", "postcode",
                    "current-energy-rating", "current-energy-efficiency",
                    "property-type", "built-form", "total-floor-area", "lodgement-date"]) \
    .apply_mapping([
        ("lmk-key", "string", "lmk_key", "string"),
        ("address1", "string", "address1", "string"),
        ("address2", "string", "address2", "string"),
        ("address3", "string", "address3", "string"),
        ("postcode", "string", "postcode", "string"),
        ("current-energy-rating", "string", "current_energy_rating", "string"),
        ("current-energy-efficiency", "string", "current_energy_efficiency", "string"),
        ("property-type", "string", "property_type", "string"),
        ("built-form", "string", "built_form", "string"),
        ("total-floor-area", "string", "total_floor_area", "float"),
        ("lodgement-date", "string", "lodgement_date", "date")
    ]) \
    .dropDuplicates(["lmk_key"])

# add year_month partition column
epc_df = epc_clean.toDF().withColumn("year_month", F.date_format(F.col("lodgement_date"), "yyyy-MM"))
epc_final = DynamicFrame.fromDF(epc_df, glueContext, "epc_final")

write_silver(epc_final, f"{BUCKET}/silver/epc/", partition_keys=["year_month"])

## ----------------- 3. FIRECRAWL NEWS BRONZE → SILVER -----------------
news_raw = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": "false"},
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{BUCKET}/bronze/firecrawl/uk-local-news/2025-09-15/"], "recurse": True},
    transformation_ctx="news_raw"
)

news_clean = news_raw.select_fields(["url", "markdown"]) \
                     .apply_mapping([
                         ("url", "string", "url", "string"),
                         ("markdown", "string", "markdown", "string")
                     ]) \
                     .dropDuplicates(["url"])

# add source partition
news_df = news_clean.toDF().withColumn("source", F.regexp_extract(F.col("url"), "https?://([^/]+)", 1))
news_final = DynamicFrame.fromDF(news_df, glueContext, "news_final")

write_silver(news_final, f"{BUCKET}/silver/news/", partition_keys=["source"])

## ----------------- FINISH -----------------
job.commit()