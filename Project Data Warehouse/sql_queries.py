import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
SONG_JSONPATH = "song_json_path.json"
S3_REGION = config.get("S3","S3_REGION")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
    );
    
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL DISTKEY,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE users(
    user_id INT PRIMARY KEY DISTKEY SORTKEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR,
    level VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id  VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL DISTKEY,
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY SORTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
    );
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR
);
""")

# STAGING TABLES
staging_events_copy = (f"""
COPY staging_events
FROM '{LOG_DATA}'
IAM_ROLE '{ARN}'
REGION '{S3_REGION}'
JSON '{LOG_JSONPATH}';
""")

staging_songs_copy = (f"""
COPY staging_songs
FROM '{SONG_DATA}'
IAM_ROLE '{ARN}'
REGION '{S3_REGION}'
JSON 'auto';
""")

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
                se.userId AS user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId AS session_id,
                se.location,
                se.userAgent as user_agent
FROM staging_events AS se
JOIN staging_songs AS ss ON (se.artist = ss.artist_name AND se.song = ss.title)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id, 
                firstName as first_name,
                lastName as last_name,
                gender,
                level
FROM staging_events
WHERE page='NextSong' AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL; 
""")

artist_table_insert = ("""
INSERT INTO artists( artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs 
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                EXTRACT (hour FROM start_time) AS hour,
                EXTRACT (day FROM start_time) AS day,
                EXTRACT (week FROM start_time) AS week,
                EXTRACT (month FROM start_time) AS month,
                EXTRACT (year FROM start_time) AS year,
                EXTRACT (weekday FROM start_time) AS weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# ANALYTICAL QUERIES
analytical_queries = {
    "Most Played Songs": """
    SELECT s.title, a.name as artist_name, COUNT(*) as play_count
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY s.title, a.name
    ORDER BY play_count DESC
    LIMIT 5;
    """,
    
    "Peak Usage Hours": """
    SELECT t.hour, COUNT(*) as play_count
    FROM songplays sp
    JOIN time t ON sp.start_time = t.start_time
    GROUP BY t.hour
    ORDER BY play_count DESC;
    """,
    
    "User Activity by Day of Week": """
    SELECT t.weekday, COUNT(*) as play_count
    FROM songplays sp
    JOIN time t ON sp.start_time = t.start_time
    GROUP BY t.weekday
    ORDER BY play_count DESC;
    """,
    
    
    "Artist Popularity by Year": """
    SELECT s.year, a.name as artist_name, COUNT(*) as play_count
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    JOIN artists a ON sp.artist_id = a.artist_id
    WHERE s.year IS NOT NULL
    GROUP BY s.year, a.name
    ORDER BY s.year DESC, play_count DESC
    LIMIT 10;
    """
}