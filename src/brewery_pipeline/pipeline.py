"""AWS Glue version of the Open Brewery Bronze -> Silver -> Gold pipeline.

Design improvements made while extracting this from the notebook:
- The module is import-safe; execution happens only through main().
- A caller supplies the batch ID, making a logical retry traceable.
- Spark setup uses the session managed by AWS Glue.
- Delta write and Gold-refresh helpers receive their Spark session and paths
  explicitly instead of relying on notebook globals.

Bronze and audit outputs use insert-only Delta MERGEs, so a retry with the same
batch ID and source record ID becomes a no-op.
"""


import logging
import requests
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType, TimestampType
from delta.tables import DeltaTable

from pyspark.sql import functions as F


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

#Pipeline Helpers
def fetch_breweries(page: int, per_page: int) -> list[dict]:
    base_url = 'https://api.openbrewerydb.org/v1/breweries'
    headers = {"Content-Type":"application/json", "Accept":"application/json"}
    params = {"page":page, "per_page":per_page, }

    try:
        response = requests.get(
            url=base_url, headers=headers, params=params, timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout as err:
        logging.error(f"Request Timed Out Error: {err}")
        raise
    except requests.exceptions.HTTPError as err:
        logging.error(f"Request HTTP Error : {err}")
        raise
    except requests.exceptions.RequestException as err:
        logging.error(f"An error occurred: {err}")
        raise
# x=fetch_breweries(1,10)
# print(len(x),x[0]['id'])

def to_bronze_df(spark_session, records: list[dict], batch_id:str):
    """Apply bronze_schema and add ingested_at."""
    bronze_schema = StructType(
        [
            StructField('id',StringType(), True),
            StructField('name',StringType(),True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), True),
            StructField("street", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("ingested_date", DateType(), True),
            StructField("batch_id", StringType(), True),
        ]
    )
    bronze_df = spark_session.createDataFrame(records, schema=bronze_schema).withColumns(
        {
            "ingested_at":F.current_timestamp(),
            "ingested_date":F.current_date(),
            "batch_id": F.lit(batch_id)
        }
        )
    return bronze_df

# bronze_brews = to_bronze_df(x)
# bronze_brews.show()

def profile_nulls(bronze_df):
    """Return a one-row null-count report."""
    # [list comprehension](generator){single value is set comprehension}{key:value is dictionary comprehension}
    #for tuple comprehension, a generator is passed inside tuple(generator)
    # for row in x:
    #     for field in row.keys():
    #         yield field
    #below is just above loop written in a single line, yielded value first, then outer loop, then inner loop
    # all_fields = {field for row in x for field in row.keys()} #it is allowed to skip the generato's paranthesis here since it is the only input, kept for brevity's sake
    # print(all_fields)
    # Though I just realised above is moot because I need to use bronze_df only to create this
    
    null_cols_expression = [F.sum(F.col(input_col).isNull().cast('int')).alias(f'{input_col}_null_count') for input_col in bronze_df.columns]
    # print(null_cols_expression)
    return bronze_df.agg(F.first("batch_id").alias("batch_id"), F.count('*').alias('total_row_count'), *null_cols_expression)
    
    
# profile_nulls(bronze_brews).show()

def split_valid_and_quarantine(bronze_df):
    """Return valid_df, quarantine_df."""
    from functools import reduce
    required_fields = ["id", "name", "city", "country"]
    missing_required = reduce(
        lambda left,right:left|right,
        [
            F.col(column).isNull() | (F.trim(F.col(column))=="")
            for column in required_fields
        ]
    )
    # bronze_df.filter(missing_required) this basically shows all rows where any of the required columns is null
    #so naturally if we take complement of it, it would show all rows where all of the required columns are not null
    #above statment happens because of Morgan's law of boolean algebra shown below
    # NOT (id missing OR name missing OR city missing OR country missing)
    # =
    # id present AND name present AND city present AND country present
    valid_df = bronze_df.filter(~missing_required)

    quarantine_df = bronze_df.filter(missing_required).withColumn('quarantine_reason', F.lit('a required column is missing'))

    return valid_df, quarantine_df

    
    # print(valid_df.count())
    # print(missing_required)
# a,b = split_valid_and_quarantine(bronze_brews)
# print(a.count(), b.count())

def deduplication(valid_df):
    duplicate_ids_df = valid_df.groupBy('id').agg(F.count('*').alias('dupe_count'), F.first("batch_id").alias("batch_id"),).filter(F.col('dupe_count')>1)
    # above it could also be "dupe_count > 1" but it cannot be "dupe_count">1 or lit(1), it needs to be one whole expression
    deduped_df = valid_df.dropDuplicates(['id'])
    return duplicate_ids_df,deduped_df
# c,d = deduplication(a)
# print(c.count(), d.count())

def build_curated(deduped_df):
    """Deduplicate IDs and apply curated transformations."""
    enriched_df = deduped_df.withColumns(
        {
            "name": F.trim(F.col("name")),
            "city": F.trim(F.col("city")),
            "country": F.trim(F.col("country")),
            "has_coordinates": (
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            ),
        }
    )

    return enriched_df.select(
        "id",
        "name",
        "brewery_type",
        "city",
        "state_province",
        "postal_code",
        "country",
        "latitude",
        "longitude",
        "website_url",
        "ingested_at",
        "has_coordinates",
        "batch_id"
        )
    
def build_gold(silver_df):
    aggregated = silver_df.groupBy("country", "brewery_type").agg(
        F.count("*").alias("brewery_count"),
        F.sum(
            F.when(F.col("has_coordinates"), F.lit(1)).otherwise(F.lit(0))
        ).alias("breweries_with_coordinates"),
    )

    return (
        aggregated
        .withColumn(
            "coordinate_coverage_pct",
            F.col("breweries_with_coordinates")
            / F.col("brewery_count")
            * 100,
        )
        .orderBy(F.col("brewery_count").desc())
    )

def refresh_gold(spark_session, curated_path: str, gold_path: str) -> None:
    silver_df = spark_session.read.format("delta").load(curated_path)
    gold_df = build_gold(silver_df)
    overwrite_to_delta(gold_df, gold_path)
# curated_df = build_curated(d)
# print(curated_df.count())
# curated_df.printSchema()

#Pipelein run definition
def run_pipeline(spark_session, page: int, per_page: int, batch_id: str) -> dict[str, object]:
    records = fetch_breweries(page, per_page)
    bronze_df = to_bronze_df(spark_session,records,batch_id)
    null_report_df = profile_nulls(bronze_df)
    valid_df,quarantine_df = split_valid_and_quarantine(bronze_df)
    duplicate_ids_df,deduped_df = deduplication(valid_df)
    curated_df = build_curated(deduped_df)
    # gold_df = build_gold(curated_df)
    return {
    "batch_id":batch_id,
    "bronze": bronze_df,
    "null_report": null_report_df,
    "valid": valid_df,
    "quarantine": quarantine_df,
    "duplicate_ids": duplicate_ids_df,
    "curated": curated_df
    # "gold": gold_df
    }
    # Bronze is written before quality filtering.
    # Then profile, validate, curate, and write outputs.

#Pipeline Write helpers and main definition
def build_paths(environment: str) -> dict[str, str]:
    """Return the S3 Delta-table paths for one deployment environment."""
    return {
        "bronze": (
            f"s3://lead-de-{environment}-bronze-data/"
            "brewery_pipeline/bronze_breweries"
        ),
        "quarantine": (
            f"s3://lead-de-{environment}-bronze-data/"
            "brewery_pipeline/quarantined_breweries"
        ),
        "null_report": (
            f"s3://lead-de-{environment}-bronze-data/"
            "brewery_pipeline/null_reports"
        ),
        "duplicate_ids": (
            f"s3://lead-de-{environment}-bronze-data/"
            "brewery_pipeline/duplicate_ids"
        ),
        "curated": (
            f"s3://lead-de-{environment}-silver-data/"
            "brewery_pipeline/curated_breweries"
        ),
        "gold": (
            f"s3://lead-de-{environment}-gold-data/"
            "brewery_pipeline/gold_brewery_summary"
        ),
    }

def merge_curated(spark_session, df, path: str) -> None:
    """Insert new curated breweries and refresh existing ones by ID."""
    if DeltaTable.isDeltaTable(spark_session, path):
        (
            DeltaTable.forPath(spark_session, path)
            .alias("target")
            .merge(
                df.alias("source"),
                "target.id = source.id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)

def merge_insert_only(
    spark_session,
    df,
    path: str,
    merge_condition: str,
    partition_columns: list[str] | None = None,
) -> None:
    """Insert only source rows not already recorded by the logical batch key."""
    if DeltaTable.isDeltaTable(spark_session, path):
        (
            DeltaTable.forPath(spark_session, path)
            .alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    writer = df.write.format("delta").mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(path)

def overwrite_to_delta(df, path: str)-> None:
    df.write.format('delta').mode('overwrite').option('overwriteSchema','True').save(path)

def write_pipeline_outputs(spark_session, outputs: dict, paths: dict[str, str]) -> None:
    """Persist one completed pipeline run to Delta tables."""

    # Retry-safe raw/audit writes. The Open Brewery source supplies a stable id.
    merge_insert_only(
        spark_session,
        outputs["bronze"],
        paths["bronze"],
        "target.batch_id = source.batch_id AND target.id = source.id",
        partition_columns=["ingested_date"],
    )
    merge_insert_only(
        spark_session,
        outputs["quarantine"],
        paths["quarantine"],
        "target.batch_id = source.batch_id AND target.id = source.id",
    )
    merge_insert_only(
        spark_session,
        outputs["null_report"],
        paths["null_report"],
        "target.batch_id = source.batch_id",
    )
    merge_insert_only(
        spark_session,
        outputs["duplicate_ids"],
        paths["duplicate_ids"],
        "target.batch_id = source.batch_id AND target.id = source.id",
    )
    # Merge curated breweries by id:
    # update existing IDs and insert new IDs.
    merge_curated(spark_session, outputs["curated"], paths["curated"])