# Project 3: Data Warehouse

## Project Description

A music streaming startup, **Sparkify**, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Database Design

![alt text](https://raw.githubusercontent.com/cm-amaya/udacity_de_nd_project3/main/images/er.png)

This database constitutes a recollection of user activity while listening to music in Sparkify, this could enable to gain insights on trends, personal preferences, and even possible recommendations. This data could be used to explore new features, such as song recommendations, in the startup and ultimately improve the user experience.

The database schema was designed taking into mind the raw data, the tables respect the original format of the data to make the ETL process easier. This is why the Artist's and Song's Ids are kept as strings and gender is keep as a single char. The fact table is `songplays`, which contains the log for the user activity in the Sparkify app, the table contains the IDs to the corresponding dimension tables which are: `users`, `songs`, `artists` and `time`.

We also include staging tables, were the raw data is loaded from two different types of sources. Based on the staging tables `staging_events` and `staging_songs`, the data is loaded unto the fact and dimensional tables.

## ETL Pipeline

 In terms of the ETL pipeline, the process is quite simple, as we copy the files containing the raw data onto the loading tables, then we insert into the star schema doing simple transformations to ensure the data types for each table.  

## Project Files

    .
    ├──sql_queries.py: Contains the queries in charge of droping, creating and inserting rows into the loading tables or regular tables from the Sparkify database.
    ├──create_tables.py: Executes the drop and create table statements that are contained in sql_queries.py. Must be run anytime a change is made in the etl.py 
    ├──etl.py: Script in charge of running the ETL pipeline, loading the raw data into the loading tables and inserting the data into the star schema.
    └──test.ipynb: Jupyter notebook that allows to test the creation of the tables and ETL process by querying the tables

## How to run the Project

Run the `create_tables.py` script to create the database and create the corresponding tables, then run the `etl.py` to load the data, finally test the ETL process with `test.ipynb`. After running `test.ipynb`, restart the kernel, to close the SQL connection.
