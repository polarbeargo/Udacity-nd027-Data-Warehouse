# Udacity-nd027-Data-Warehouse  


## Project Datasets
* Song data: 's3://udacity-dend/song_data'  
* Log data: 's3://udacity-dend/log_data' 

## Project Template
1. **reate_table.py** is where you'll create your fact and dimension tables and staging tables for the star schema in Redshift.  
2. **etl.py** is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.  
3. **sql_queries.py** is where you'll define you SQL statements, which will be imported into the two other files above.  
4. **create_redshift_cluster.ipynb** is where you'll create redshift cluster and create an IAM role that has read access to S3.  
 5. **README.md** is where you'll provide discussion on your process and decisions for this ETL pipeline. 

## How to run scripts

Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

Choose `DB/DB_PASSWORD` in `dhw.cfg`.

Create IAM role, Redshift cluster, and configure TCP connectivity  

Drop and recreate tables

```bash
$ python create_tables.py
```

Run ETL pipeline

```bash
$ python etl.py
```  
Delete IAM role and Redshift cluster
