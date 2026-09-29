import json
from pathlib import Path

from brewery_pipeline.pipeline import (
    split_valid_and_quarantine,
    to_bronze_df,
)
def test_to_bronze_df_preserves_source_data_and_adds_run_metadata(spark):
    fixture_path = Path(__file__).parent / "fixtures" / "breweries_valid.json"
    records = json.loads(fixture_path.read_text())

    bronze_df = to_bronze_df(
        spark,
        records,
        batch_id="test-batch-001",
    )

    row = bronze_df.first()

    assert bronze_df.count() == 1
    assert row["id"] == "test-brewery-001"
    assert row["name"] == "Test Brewery"
    assert row["batch_id"] == "test-batch-001"
    assert row["ingested_at"] is not None
    assert row["ingested_date"] is not None

def test_missing_required_id_is_quarantined(spark):
    fixture_path = Path(__file__).parent / "fixtures" / "breweries_invalid.json"
    records = json.loads(fixture_path.read_text())

    bronze_df = to_bronze_df(
        spark,
        records,
        batch_id="test-batch-invalid-001",
    )
    valid_df, quarantine_df = split_valid_and_quarantine(bronze_df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 1

    row = quarantine_df.first()
    assert row["id"] is None
    assert row["quarantine_reason"] == "a required column is missing"