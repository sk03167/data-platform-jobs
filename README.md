# Data Platform Jobs

Deployable data-processing jobs for the learning data platform.

## Current job

`brewery_pipeline` fetches Open Brewery DB data and produces retry-safe Bronze, Silver, and Gold Delta outputs.

## Repository boundary

- This repository owns job code, tests, and artifact publishing.
- `learn-terraform-get-started-aws` owns AWS infrastructure, IAM, and Glue job configuration.