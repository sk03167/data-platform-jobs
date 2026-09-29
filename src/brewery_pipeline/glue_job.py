"""AWS Glue entry point for the Open Brewery batch pipeline."""

import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from brewery_pipeline.pipeline import (
    build_paths,
    refresh_gold,
    run_pipeline,
    write_pipeline_outputs,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def create_glue_spark():
    """Return the Spark session managed by this Glue job."""
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    glue_context.spark_session.sparkContext.setLogLevel("WARN")
    return glue_context, glue_context.spark_session


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "environment", "page", "per_page", "batch_id"],
    )

    glue_context, spark_session = create_glue_spark()
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    outputs = run_pipeline(
        spark_session,
        page=int(args["page"]),
        per_page=int(args["per_page"]),
        batch_id=args["batch_id"],
    )
    paths = build_paths(args["environment"])
    write_pipeline_outputs(spark_session, outputs, paths)
    refresh_gold(spark_session, paths["curated"], paths["gold"])

    logging.info("Completed batch %s", outputs["batch_id"])
    job.commit()


if __name__ == "__main__":
    main()