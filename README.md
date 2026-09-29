# Data Platform Jobs

Deployable data-processing jobs for the learning data platform.

## Current job

`brewery_pipeline` fetches Open Brewery DB data and produces retry-safe Bronze, Silver, and Gold Delta outputs.

## Repository boundary

- This repository owns job code, tests, and artifact publishing.
- `learn-terraform-get-started-aws` owns AWS infrastructure, IAM, and Glue job configuration.

## Dependencies

- `requests==2.32.3` is used to call the Open Brewery DB API.
- AWS Glue supplies PySpark, Delta Lake, and Glue libraries at runtime, so they are intentionally not listed in `requirements.txt`.