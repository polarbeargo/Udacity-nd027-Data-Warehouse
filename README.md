# Udacity-nd027-Data-Warehouse  


## Project Datasets
* Song data: 's3://udacity-dend/song_data'  
* Log data: 's3://udacity-dend/log_data' 

## Fact Table  
1. songplays - records in log data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  
## Dimension Tables  

2. users - users in the app  
   * user_id, first_name, last_name, gender, level  
3. songs - songs in music database    
   * song_id, title, artist_id, year, duration  
4. artists - artists in music database   
   * artist_id, name, location, latitude, longitude  
5. time - timestamps of records in songplays broken down into specific units  
   * start_time, hour, day, week, month, year, weekday  
  
## Project Template
1. `create_table.py` is where you'll create your fact and dimension tables and staging tables for the star schema in Redshift.  
2. `etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.  
3. `sql_queries.py` is where you'll define you SQL statements, which will be imported into the two other files above.  
4. `create_redshift_cluster.ipynb` is where you'll create redshift cluster and create an IAM role that has read access to S3.  
5. `README.md` is where you'll provide discussion on your process and decisions for this ETL pipeline.  

## Project Steps
Below are steps you can follow to complete each component of this project.

#### Create Table Schemas
1. Design schemas for your fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in `sql_queries.py`
3. Complete the logic in `create_tables.py` to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of `create_tables.py` if the tables already exist. This way, you can run `create_tables.py` whenever you want to reset your database and test your ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to `dwh.cfg`.
7. Test by running `create_tables.py` and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.
#### Build ETL Pipeline
1. Implement the logic in `etl.py` to load data from S3 to staging tables on Redshift.
2. Implement the logic in `etl.py` to load data from staging tables to analytics tables on Redshift.
3. Test by running `etl.py` after running `create_tables.py` and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete your redshift cluster when finished.

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
