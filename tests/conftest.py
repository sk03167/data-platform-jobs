import pytest
from pyspark.sql import SparkSession
import os
import sys
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
    SparkSession.builder
    .appName("brewery-pipeline-tests")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()

    yield session

    session.stop()