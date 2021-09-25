import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
"""
Song Data and Log Data files are stored on AWS S3 which will be copied to staging_events table using COPY function, Data from this table is transformed and stored in other tables for analysis purpose
"""
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR(2),
    iteminSession INTEGER,
    lastName VARCHAR(50),
    length DECIMAL,
    level VARCHAR NOT NULL,
    location VARCHAR, 
    method VARCHAR,
    page VARCHAR,
    registration DECIMAL,
    sessionId INTEGER NOT NULL,
    song VARCHAR,
    status INTEGER,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId INTEGER);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR NOT NULL,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    duration DECIMAL NOT NULL,
    year INTEGER);

""")

#songplays is the Fact table in this Star schema

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER,
    level VARCHAR NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR NOT NULL);
""")

#users is the dimension table

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR(50), 
    last_name VARCHAR(50), 
    gender CHAR(2), 
    level VARCHAR);
""")

#songs is the dimension table

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR PRIMARY KEY , 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INTEGER,  
    duration DECIMAL);
""")

#artists is the dimension table

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude DECIMAL, 
    longitude DECIMAL);
""")

#time is the dimension table

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER);
""")

# STAGING TABLES

"""
Copying log data from S3 bucket,Files are in JSON format.
Source data path, IAM Role ARN, and JSON path file details are sourced from config file using config parser
"""
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    FORMAT AS json {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

"""
Copying song data from S3 bucket,Files are in JSON format.
Source data path, IAM Role ARN, and JSON path file details are sourced from config file using config parser
"""
staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    FORMAT AS json 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time,user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time,
        se.userId,
        se.level, 
        ss.song_id,
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
FROM staging_events se
JOIN staging_songs ss
ON (se.artist = ss.artist_name) 
AND (se.song = ss.title)
AND (se.length = ss.duration)
WHERE se.page = 'NextSong';                       
""")

user_table_insert = ("""
INSERT INTO users 
SELECT DISTINCT se.userId AS user_id, 
        se.firstName AS first_name, 
        se.lastName AS last_name, 
        gender, 
        level
FROM staging_events se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs 
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists 
SELECT DISTINCT artist_id, 
        ss.artist_name AS name, 
        ss.artist_location AS location, 
        ss.artist_latitude AS latitude, 
        ss.artist_longitude AS longitude
FROM staging_songs ss;
""")

"""
Redshift don't have any pre-defined method to convert string to timestamp.
Redshift timestamp epoch equals 1970-01-01 00:00:00.000000 ,
Interval adds specified date, time, and day values to any date,
ts represents timestamp from events (log) files which is stored in staging_events table as BIGINT,
ts is in miliseconds hence it is divided by 1000 to convert to seconds and then multiplied 
by "Interval '1 second'", resulting in the interval of ts seconds since epoch
Result from interval calc is added to epoch which returns the proper Redshift Timestamp for ts
"""

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEKS FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(dayofweek from start_time) AS weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
