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

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    iteminSession INTEGER,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration NUMERIC,
    year INTEGER);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR);

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INTEGER,  
    duration NUMERIC);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude NUMERIC, 
    longitude NUMERIC);
""")

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

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    FORMAT AS json {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

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
SELECT (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time,
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

# songplay_table_insert = ("""
# INSERT INTO songplays
# SELECT se.ts AS start_time, 
#         se.userId AS user_id,
#         se.level AS level, 
#         ss.song_id AS song_id,
#         ss.artist_id AS artist_id, 
#         se.sessionId AS session_id, 
#         se.location AS location, 
#         se.userAgent AS user_agent
# FROM staging_events se
# JOIN staging_songs ss
# ON (se.artist = ss.artist_name) 
# AND (se.song = ss.title)
# AND (se.length = ss.duration)
# WHERE se.page = 'NextSong';                       
# """)

user_table_insert = ("""
INSERT INTO users 
SELECT se.userId AS user_id, 
        se.firstName AS first_name, 
        se.lastName AS last_name, 
        gender, 
        level
FROM staging_events se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs 
SELECT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists 
SELECT artist_id, 
        ss.artist_name AS name, 
        ss.artist_location AS location, 
        ss.artist_latitude AS latitude, 
        ss.artist_longitude AS longitude
FROM staging_songs ss;
""")

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
