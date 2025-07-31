Welcome to Rome2Rio - Lead Analytics Engineer Take-Home Case Study

## Part 1: Build the Solution - Data Pipeline

### Requirements
Python 3.12

### Set up a virtual environment

*For Mac*
```
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

*For Windows*
```
python -m venv env
env\Scripts\activate
pip install -r requirements.txt
```

## Install dbt dependencies
`dbt deps`

### Run dbt and write data to DuckDB
`dbt build --profiles-dir profiles`

### View data in DuckDB UI
`duckdb data/rome2rio_target.duckdb -ui -readonly`

## Part 2: Code Review – Mentoring a Junior Engineer

Feedback:
- **Business requirements**: What's the business question this model answers? Consider filtering for active bookings (`WHERE status = 'confirmed'`) or specific date ranges to match business needs.

- **Data accuracy**: Use `count(distinct p.passengerid)` instead of `count(*)` to avoid inflated passenger counts when passengers have multiple tickets.

- **JOIN strategy**: Use `LEFT JOIN` to keep all bookings since tickets can be issued after a booking is made.

- **Column clarity**: Prefix columns with table aliases (`b.bookingid` instead of `bookingid`) to avoid ambiguity.

- **Code standards**: Run `sqlfluff fix` to standardise formatting (e.g., separate lines for columns, JOIN indentation).

- **Data modeling**: Consider referencing curated models instead of raw tables, as these data needs cleaning before analysis.

## Part 3: Technical Leadership - Decision Scenarios

*B) Schema Changes: Marketing wants 15 new tracking fields*

- **Model versioning**: Create versioned models (e.g. `dim_passenger_v1`, `dim_passenger_v2`) to manage data product lifecycle (pre-release, active, end of life, deprecated), and build a compatibility matrix of data fields supported by which model versions.

- **Data validation**: Build a test plan with Marketing stakeholders, and implement dbt unit & data tests for business logical validation.

- **Pre-deployment checklist**: Submit a pull request for peer review; run automated CI/CD checks; dry run data models with dev or syndicated data in pre-prod environment.

- **Governance & change management**: 1) Add dbt model contracts, evaluate privacy & security settings (PII data masking and policy-tagging, IAM controls, row-level security); 2) initiate a change request process with platform and architecture teams for impact and privacy assessments, and 2) ask for Marketing stakeholders for sign-offs. 
 
 - **Phased rollout**: Repoint Marketing's internal dashboards to the latest model version before expanding to executive reporting and granting production access.

 - **User enablement**: Update data dictionary and schedule knowledge sharing workshops for Marketing team.

 - **Monitoring & observability**: Integrate `dbt-elementary` into the workflow to monitor data SLAs and notify the metric owners of data health issues.
