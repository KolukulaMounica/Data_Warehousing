# Data_Warehousing
A Cloud Data Warehousing project on sparkify data.

 - create_tables.py & etl.py
   - Establish a connection
     - Using the credentials in dwh.cfg file such as the host(endpoint), dbname, user, password, port number, we can establsh a connection and thn execute the queries.

 - sql_queries.py helps in creating tables, staging the data and loading the data into the tables created.
   - step 1: Drop all the existing tables, to work on the fresh tables and fresh data.
   - step 2: Create all the tables with the required fields. (staging_events, staging_songs, songplays, users, songs, artists, time)
   - step 3: Staging the tables using the s3 bucket URL and IAM role credentials.
   - step 4: Inserting all the data into these created tables.

These are the steps used in Cloud Data Warehousing using Redshift cluster, IAM roles, database configurations.