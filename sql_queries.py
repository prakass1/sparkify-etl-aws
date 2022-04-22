import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")


# Region
REGION = config.get("CLUSTER", "REGION")

# ARN
IAM_ARN = config.get("IAM_ROLE", "ARN")

# S3
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_DATA_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    first_name TEXT,
    gender TEXT,
    item_in_session INT,
    last_name TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration FLOAT,
    session_id INT,
    song TEXT,
    status INT,
    ts VARCHAR,
    user_agent TEXT,
    user_id INT
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_location TEXT,
    artist_longitude FLOAT,
    artist_name TEXT,
    duration FLOAT,
    num_songs INT,
    song_id TEXT,
    title TEXT,
    year INT
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP SORTKEY DISTKEY NOT NULL,
    user_id INT NOT NULL REFERENCES users(user_id),
    level TEXT,
    song_id TEXT NOT NULL REFERENCES songs(song_id),
    artist_id TEXT NOT NULL REFERENCES artists(artist_id),
    session_id INT NOT NULL,
    location TEXT,
    user_agent TEXT
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT NOT NULL PRIMARY KEY,
    title TEXT,
    artist_id TEXT NOT NULL DISTKEY REFERENCES artists(artist_id),
    year INT,
    duration FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT NOT NULL PRIMARY KEY,
    name TEXT,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL SORTKEY DISTKEY PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
"""

# STAGING TABLES

staging_events_copy = (
    """
copy staging_events from {}
    iam_role '{}'
    json {} compupdate off region '{}';
"""
).format(LOG_DATA, IAM_ARN, LOG_DATA_PATH, REGION)

staging_songs_copy = (
    """
copy staging_songs from {}
    iam_role '{}'
    TRUNCATECOLUMNS json 'auto' compupdate off region '{}';
"""
).format(SONG_DATA, IAM_ARN, REGION)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + cast(ts AS bigint)/1000 * interval '1 second' as start_time,
se.user_id as user_id,
se.level as level,
ss.song_id as song_id,
ss.artist_id as artist_id,
se.session_id as session_id,
se.location as location,
se.user_agent as user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.artist = ss.artist_name AND se.song = ss.title)
WHERE se.page = 'NextSong';
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(user_id) as user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page='NextSong';
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id) as song_id, title, artist_id, year, duration
FROM staging_songs;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) as artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + cast(ts AS bigint)/1000 * interval '1 second' as start_time,
EXTRACT(hour from start_time) as hour,
EXTRACT(day from start_time) as day,
EXTRACT(week from start_time) as week,
EXTRACT(month from start_time) as month,
EXTRACT(year from start_time) as year,
EXTRACT(dayofweek from start_time) as weekday
FROM staging_events;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    song_table_drop,
    artist_table_drop,
    user_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    time_table_insert,
    artist_table_insert,
]

## Analyze queries
songs_query = """
SELECT s.title, a.name, a.location FROM songs s
JOIN artists a ON a.artist_id = s.artist_id
WHERE year BETWEEN 2000 and 2005 LIMIT 10;
"""
artists_query = """
SELECT COUNT(*) FROM artists;
"""
users_query = """
SELECT COUNT(*) FROM users;
"""
time_query = """
SELECT COUNT(*) FROM time;
"""
songplays_query = """
SELECT COUNT(*) FROM songplays;
"""
songs_count_query = """
SELECT COUNT(*) FROM songs;
"""
songplays_query_windows = """
SELECT COUNT(*) as windows_users
FROM songplays
WHERE songplays.user_agent LIKE '%Windows%'; 
"""
songplays_query_mac = """
SELECT COUNT(*) as mac_users
FROM songplays
WHERE songplays.user_agent LIKE '%Mac OS%'; 
"""

analyze_queries = [
    songs_query,
    artists_query,
    songs_count_query,
    users_query,
    time_query,
    songplays_query,
    songplays_query_windows,
    songplays_query_mac,
]
