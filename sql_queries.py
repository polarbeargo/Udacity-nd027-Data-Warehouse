import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events(
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        ItemInSession INT,
        lastName TEXT,
        length FLOAT,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration TEXT,
        sessionId INT,
        song TEXT,
        status INT,
        ts BIGINT, 
        userAgent TEXT, 
        userId INT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
        song_id TEXT PRIMARY KEY,
        artist_id TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name VARCHAR(255),
        duration FLOAT,
        num_songs INT,
        title TEXT,
        year INT
    )
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR PRIMARY KEY NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
"""

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
