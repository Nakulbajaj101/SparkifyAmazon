# Sparkify Music Analytical Engine

The purpose of the project is to build a database and design an
ETL pipeline that would power the analytical needs for Sparkify.

## Motivation

A startup called Sparkify wants to analyze the data they've been
collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what
songs users are listening to. Currently, they don't have an easy way
to query their data, which resides in a directory of JSON logs on user
activity on the app, as well as a directory with JSON metadata on the
songs in their app. In order to provide better service to their users,
Sparkify would like to bring their data to a data store/database so
analysts can perform queries faster, and analyse user patterns in
an optimized manner.


## Built With

The section covers tools used to enable the project.

1. Redshift to store data and s3 for backup data
2. Python to process and carry out ETL
3. Python to create the infrastructure and delete the infrastructure

## Schema for Song Play Analysis

Using the song and log datasets, created a star schema optimized for
queries on song play analysis. This includes the following tables listed
below. Using this schema majority of data required for analysis can be
in few tables, and only few joins may have to be used. This is a common
design utilised in industry which maintains a balance of optimum
normalisation with enhanced performance. Since the goal is having a
database which analysts can use efficiently, focus was on optimisation
rather than storage reduction and redundancy. The fact table is distributed 
using the key across all 4 nodes in the cluster where as dimension tables are 
duplicated across the 4 nodes in the cluster as these tables are small, 
and can optimize the speed of analysis. 

### Fact Table

1. songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

2. users - users in the app
- user_id, first_name, last_name, gender, level

3. songs - songs in music database
- song_id, title, artist_id, year, duration

4. artists - artists in music database
-  artist_id, name, location, latitude, longitude

5. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday


## Files and Folders

1. create_tables.py - Contains logic to clean up and recreate database with tables.
2. sql_queries.py - Contains table definitions, DDL/DML SQL scripts to create tables, drop table, select data from table and insert data into tables.
3. etl.py - Contains all the logic to extract data from S3 and load data into staging table in redshift songs database and from staging tables to dimension and fact table.
4. creatinginfrastructure.py - Contains the pipeline to automate infrastructure requirements which will create the redshift cluster, role access to read data and load data from s3 into the redshift cluste
5. deletinginfrastructure.py - Contains the pipeline to destroy the infratsructure associated with the project.

## Running the ETL pipeline

1. Create the editor role in aws iam
2. Open a jupyter notebook and run the following code twice
   with input your aws_access_key and secret_key:
    ```python
    import base64 
  
    sample_string = input() #input aws key and secret
    sample_string_bytes = sample_string.encode("ascii") 
    base64_bytes = base64.b64encode(sample_string_bytes) 

    base64_string = base64_bytes.decode("ascii")
    print(f"Encoded string: {base64_string}")
    ```   
3. Create a aws_editor.json file with following keys:
   ```json
   {"access_key":"<encoded aws_access_key>",
   "secret_key":"<encoded aws_secret_key>"}
   ```
4. Save the aws_editor.json file on the root user path:
   `/Users/<systemUser>/aws_editor.json`
5. Open terminal
6. Run createConfig.py script to create the config file that will be used to spin the infratstructure and will have details to connect to redshift and insert data into it.

    `python3 createConfig.py`

7. Run the creatingInfrastructure.py to get the infrastructure ready.

    `python3 creatingInfrastructure.py`

8. Run create_tables.py script to clean the database and recreate database and table definitions.

    `python3 create_tables.py`

9. Run etl.py to extract data from S3 and load into redshift database

    `python3 etl.py`

### Example of fetching data from database

Log to the aws console and go to the redshift cluster:
(https://us-west-2.console.aws.amazon.com/redshiftv2/home?region=us-west-2#landing "AWS Redshift us-west-2 homepage")

Click on "Query Editor"
Enter the login details as in dwh.cfg file

Run the query below

```sql 
SELECT user_id, start_time, song_id, session_id, level FROM songs.songplays 
WHERE session_id=221
AND song_id='SODTRKW12A6D4F9A51'
LIMIT 1;
```

| user_id | start_time          | song_id            | session_id | level |
|---------|---------------------|--------------------|------------|-------|
| 15      | 2018-11-07 18:11:11 | SODTRKW12A6D4F9A51 | 221        | paid  |



## Destroying the infrastructure to avoid costs

1. Run the deletingInfrastructure.py that will destroy the cluster
   and remove the role and policty attached to the role.
   
   `python3 deletingInfrastructure.py`
 


# Contact for questions or support

Nakul Bajaj
