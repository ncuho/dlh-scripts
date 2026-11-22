# Explicit Spark business stages

Native Airflow DAG for reviewing each SQL entity as its own task. Requires a separately captured immutable canonical_event and the source boundary held by the operator. Load the reviewed SQL and Spark transport into the configured S3 DAG directory. It does not capture a cut, load history, grant access or publish BI; those are explicit separate steps. No blind retries.
