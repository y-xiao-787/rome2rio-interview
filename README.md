Welcome to Rome2Rio - Lead Analytics Engineer Take-Home Case Study

Author: Yuhuan Xiao

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

