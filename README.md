# CC4D - Cloud Composer for Data

# CC4D

## What does CC4D do?

CC4D is a scalable solution, designed to get more clients to share their 1P data
, ML predictions and offline conversions with Google Platforms (GA, Ads, etc.).
CC4D runs on
[Cloud Composer](https://cloud.google.com/composer/), using
Apache [Airflow](https://airflow.apache.org/).

## CC4D [DAG](https://airflow.apache.org/docs/stable/concepts.html#dags)

*   `cc4d_bq_to_ga`: Transfer events from an SQL table in
    [BigQuery](https://cloud.google.com/bigquery/) to
    [Google Analytics](https://analytics.google.com/analytics/web/)

*   `cc4d_gcs_to_ga`: Transfer events from
    [Google Cloud Storage (GCS)](https://cloud.google.com/storage/)
    to Google Analytics. The events may be in a JSON or CSV formatted files in
    GCS.

NOTE: BigQuery/GCS to
[Google Ads UAC](https://developers.google.com/adwords/api/docs/guides/mobile-app-campaigns)
DAGs are currently under development.
