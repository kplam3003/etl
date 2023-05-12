# Overview
This project contains scripts for manage database structure version. The framework use is [Alembic](https://alembic.sqlalchemy.org/en/latest/index.html)

# Getting Started

### 1. Create `.env` file in root directory for running local
```.env
ETL_DATABASE_URI=postgres+psycopg2://postgres:password@localhost:5433/test_data
PYTHONPATH=/path/to/data_migration
```

### 2. Install `pipenv`. [More details](https://pypi.org/project/pipenv/)
```shell
virtualenv .venv
source .venv/bin/activate

pip install pipenv
```

### 3. Install dependencies
```shell
pipenv install
```

# Usage

### 1. Run migration scripts
```shell
pipenv run alembic upgrade head
```

After run the script, the database table to track for the version applied in `public.alembic_version`

| version_num |
| ----------- |
| 41ac3d38ccc0|


### 2. Create a new migration script
```shell
pipenv run alembic revision -m "Change some thing"
```

A new migration script will be generated in `alembic/versions/{revision}_{name}.py`
```python
"""Change some thing

Revision ID: bd30e5e471f5
Revises: 41ac3d38ccc0
Create Date: 2021-01-07 09:27:26.610750

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bd30e5e471f5'
down_revision = '41ac3d38ccc0'
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
```

Put your script to make change to database in upgrade function. Also need to put downgrade script to be able to downgrade the database to the last changed version.


export $(grep -v '^#' .env | xargs)

> Test.