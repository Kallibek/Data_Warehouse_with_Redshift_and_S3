# Data Warehouse using AWS (Redshift and S3)
by Kallibek Kazbekov

---
# Project summary
## Purpose
The Sparkify startup grows and wants to move their data to cloud database, namely redshift. Their user activity log data and music meta data, both in JSON, are located in S3 bucket. 

## ETL schema
<img src="/Supplementary/ETL%20pipeline.png" alt="alt text">

## ETL pipeline
1. Redshift copies log files and metadata to the two tables of Redshift database: `staging_events` and `staging_songs`;
1. `staging_events`  was used to insert data into `songplays`,`users`;
1. `staging_songs` was used to insert data into `songs`,`artists`;
1. `songplays` was used to fill the `time` table.
---
# Project instructions on how to run the Python scripts

1. `create_tables.py` should be executed first, since it deletes and recreates the tables;
1. Then `etl.py` script copies log files and metadata from the S3 bucket to the two tables of Redshift database. Then, these two tables are used to create other tables of the database.

# An explanation of the files in the repository

### `create_tables.py`
Runs SQL queries to drop and create tables.
```python
def main():
    """
    Creates a connection to the existing Redshift cluster;

    Deletes existing tables in the database;

    Creates new tables;

    Closes the connection.

    """

def drop_tables(cur, conn):
    """
    Deletes existing tables.
    """
def create_tables(cur, conn):
    """
    Creates new tables.
    """
 ```

### `etl.py` 
Runs SQL queries to copy and insert data.
```python
def main():
    """
    1. Create a connection to the existing Redshift database;
    
    2. Copies data from S3 to the tables of the Redshift database;
    
    3. Inserts data from the two tables into other tables of the database.
    """
def load_staging_tables(cur, conn):
    """
    Copies user activity and music metadata from S3 to the tables of the Redshift database.
    """
def insert_tables(cur, conn):
    """
    Inserts data from the two copied tables into other tables of the database.
    """
```

### `sql_queries.py`
Contains SQL queries to drop and create tables, copy and insert data;

### `supplementary` directory
Directory for any supplementary data used in `README.md`.
