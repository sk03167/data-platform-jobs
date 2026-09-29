import json
from pathlib import Path

from brewery_pipeline.pipeline import (
    build_curated,
    deduplication,
    profile_nulls,
    split_valid_and_quarantine,
    to_bronze_df,
    write_pipeline_outputs,
)


def test_replaying_same_batch_does_not_duplicate_bronze_rows(spark, tmp_path):
    fixture_path = Path(__file__).parent / "fixtures" / "breweries_valid.json"
    records = json.loads(fixture_path.read_text())

    bronze_df = to_bronze_df(
        spark,
        records,
        batch_id="test-batch-001",
    )
    valid_df, quarantine_df = split_valid_and_quarantine(bronze_df)
    duplicate_ids_df, deduped_df = deduplication(valid_df)

    outputs = {
        "bronze": bronze_df,
        "null_report": profile_nulls(bronze_df),
        "quarantine": quarantine_df,
        "duplicate_ids": duplicate_ids_df,
        "curated": build_curated(deduped_df),
    }
    paths = {
        "bronze": str(tmp_path / "bronze"),
        "quarantine": str(tmp_path / "quarantine"),
        "null_report": str(tmp_path / "null_report"),
        "duplicate_ids": str(tmp_path / "duplicate_ids"),
        "curated": str(tmp_path / "curated"),
    }

    write_pipeline_outputs(spark, outputs, paths)
    write_pipeline_outputs(spark, outputs, paths)

    written_bronze = spark.read.format("delta").load(paths["bronze"])

    assert written_bronze.count() == 1
    assert written_bronze.first()["batch_id"] == "test-batch-001"