# **Implementing Data Warehouse in Redshift**

# Project Summary
The Project involves creation of a database schema and ETL pipeline for the Sparkify.
Sparkify is a music streaming startup which has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in **S3, in a directory of JSON logs** on user activity on the app, as well as a **directory with JSON metadata** on the songs in their app.

# **Project Description**
In these project we will create an ETL pipeline that extracts their data from **S3**, stages them in **Redshift**, and transforms data into **a set of dimensional tables** to find what songs their users are listening to. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# **Project Datasets**
## **Song Dataset**
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
'''
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
'''
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
'''
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
'''
