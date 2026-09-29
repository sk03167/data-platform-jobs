from brewery_pipeline.pipeline import build_paths


def test_build_paths_uses_environment_specific_buckets():
    paths = build_paths("dev")

    assert paths == {
        "bronze": (
            "s3://lead-de-dev-bronze-data/"
            "brewery_pipeline/bronze_breweries"
        ),
        "quarantine": (
            "s3://lead-de-dev-bronze-data/"
            "brewery_pipeline/quarantined_breweries"
        ),
        "null_report": (
            "s3://lead-de-dev-bronze-data/"
            "brewery_pipeline/null_reports"
        ),
        "duplicate_ids": (
            "s3://lead-de-dev-bronze-data/"
            "brewery_pipeline/duplicate_ids"
        ),
        "curated": (
            "s3://lead-de-dev-silver-data/"
            "brewery_pipeline/curated_breweries"
        ),
        "gold": (
            "s3://lead-de-dev-gold-data/"
            "brewery_pipeline/gold_brewery_summary"
        ),
    }