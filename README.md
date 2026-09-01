# E-Commerce Data Pipeline

A batch data pipeline for e-commerce transaction data, built around a three-zone
architecture (Raw → Curated → Refined) with PySpark transformations, Great
Expectations quality checks, and Airflow orchestration.

## Project status

This is a work in progress, not a deployed system. Read this section before the
rest — it describes what actually runs today.

| Component | State |
|---|---|
| `TransactionProcessor` (PySpark) | Implemented — dedup, type casting, standardization, business rules |
| `QualityValidator` (Great Expectations) | Implemented — 20 expectations across 4 quality dimensions |
| `daily_transaction_pipeline` DAG | Structure complete — task graph, sensor, retries, failure alerting |
| DAG task bodies | **Stubs.** Each `PythonOperator` callable logs and returns `True`; the real calls are written out as comments |
| Ingestion layer | Not written |
| Warehouse load (Snowflake) | Not written |
| Tests | Not written |
| Config loading | Not written — thresholds are hardcoded defaults in `QualityValidator._default_config()` |

Neither the processor nor the validator has been run against a real dataset yet,
so there are no performance numbers to report.

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  Raw Zone   │ ───> │ Curated Zone │ ───> │Refined Zone │
│    (S3)     │      │   (PySpark)  │      │ (Snowflake) │
└─────────────┘      └──────────────┘      └─────────────┘
       │                     │                      │
    Landing              Processing             Analytics
    Storage            & Validation            & BI Layer
```

The Raw → Curated hop is the part with working code. The Curated → Refined hop
exists as a DAG task and a SQL sketch in comments.

## Repository contents

```
E-commerce-data-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   ├── transformation/
│   │   └── transaction_processor.py    # Raw → Curated transforms
│   └── quality/
│       └── quality_validator.py        # Great Expectations suite
└── airflow/
    └── dags/
        └── daily_transaction_pipeline.py
```

## Components

### `TransactionProcessor`

Takes a raw transaction DataFrame and returns a `(valid_df, invalid_df)` tuple,
so bad records are quarantined rather than dropped silently.

Pipeline stages:

1. **Deduplicate** — window over `transaction_id`, keeping the row with the
   latest `ingestion_timestamp`.
2. **Cast types** — strips thousands separators from `amount` and casts to
   `DecimalType(10,2)`; parses `transaction_date_string` to a date; casts
   `quantity` to int.
3. **Standardize** — uppercases and trims `currency_code` and
   `transaction_type`, trims `customer_email`.
4. **Apply business rules** — flags each row `is_valid` and attaches a
   `validation_errors` string, then splits the frame.

Business rules enforced: critical fields non-null; `0 < amount < 100000`;
`transaction_type` in `{PURCHASE, REFUND, ADJUSTMENT}`; `currency_code` in
`{USD, EUR, GBP}`; `quantity > 0`.

### `QualityValidator`

Runs 20 Great Expectations checks grouped into four dimensions:

- **Completeness (9)** — non-null on the five critical columns, table row count
  within `[10_000, 1_000_000]`, and a 5%-null tolerance on `customer_email`,
  `product_id`, and `quantity`.
- **Accuracy (6)** — `amount` and `quantity` range checks, categorical set
  membership for currency and transaction type, an email regex at 95%
  tolerance, and a `^[A-Z0-9]{10,20}$` format check on `transaction_id`.
- **Consistency (3)** — `transaction_id` uniqueness, and compound uniqueness on
  `(customer_id, transaction_date, amount)` at 99% tolerance.
- **Timeliness (2)** — transaction dates within the last 90 days and not in the
  future.

`generate_report()` renders a pass/fail summary with the failing expectations
broken out. `main()` raises on failure, which is what would fail the DAG task.

### `daily_transaction_pipeline` DAG

Scheduled `0 2 * * *`, `catchup=False`, 2 retries with a 5-minute delay and a
2-hour task timeout.

```
S3KeySensor → ingest_to_raw → process_to_curated → load_to_warehouse → run_quality_checks → send_success_notification
                                                                                          ↘ send_failure_email
```

`send_success_notification` uses `trigger_rule='all_success'` and
`send_failure_email` uses `trigger_rule='one_failed'`, so exactly one of the two
terminal tasks fires. The sensor polls every 5 minutes for up to an hour.

As noted above, the five `PythonOperator` callables are stubs today.

## Tech stack

PySpark 3.5, Great Expectations 0.18, Apache Airflow 2.7, boto3, pandas.
Full pins in [requirements.txt](requirements.txt).

## Setup

```bash
git clone https://github.com/RohitRathod0/E-commerce-data-pipeline.git
cd E-commerce-data-pipeline
pip install -r requirements.txt
```

There is no config file or CLI entry point yet. The two modules each have a
`main()` with hardcoded S3 paths that serves as a usage example — edit the paths
before running:

```bash
python src/transformation/transaction_processor.py
python src/quality/quality_validator.py
```

To load the DAG, copy it into your Airflow home:

```bash
cp airflow/dags/daily_transaction_pipeline.py $AIRFLOW_HOME/dags/
```

## Known gaps

- `QualityValidator` uses `SparkDFDataset`, the legacy Great Expectations API.
  It works on the pinned 0.18 but is removed in GX 1.x, so a version bump means
  a rewrite against the modern Fluent Datasource API.
- The DAG's `sys.path` does not include `src/`, so wiring the real imports needs
  either a package install or a path shim.
- `TransactionProcessor.process()` calls `.count()` four times for logging,
  which forces repeated evaluation of the lineage. Worth caching or dropping
  once it runs on real volumes.
- The consistency suite re-asserts the same `amount` range check that the
  accuracy suite already covers.

## Roadmap

- [ ] Ingestion layer with schema validation on landing
- [ ] Real DAG task bodies wired to the processor and validator
- [ ] Snowflake load
- [ ] pytest suite with a small fixture dataset
- [ ] YAML config loading to replace hardcoded thresholds
- [ ] Benchmark on a generated dataset and publish real numbers

## License

MIT

## Contact

Rohit Rathod
- GitHub: [@RohitRathod0](https://github.com/RohitRathod0)
