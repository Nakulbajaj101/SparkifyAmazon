import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# SCHEMA NAMES

songs_schema = 'songs'

# TABLE NAMES

staging_events_table = 'staging_events'
staging_songs_table = 'staging_songs'
songplay_table = 'songplays'
user_table = 'users'
song_table = 'songs'
artist_table = 'artists'
time_table = 'time'

# DROP TABLES

staging_events_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{staging_events_table};"
staging_songs_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{staging_songs_table};"
songplay_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{songplay_table};"
user_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{user_table};"
song_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{song_table};"
artist_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{artist_table};"
time_table_drop = f"DROP TABLE IF EXISTS {songs_schema}.{time_table};"

# CREATING SCHEMAS

songs_schema_create = f"""CREATE SCHEMA IF NOT EXISTS {songs_schema};"""

#DROPPING SCHEMAS

songs_schema_drop = f"""DROP SCHEMA IF EXISTS {songs_schema};"""

# CREATE TABLES

staging_events_table_create= (f"""CREATE TABLE {songs_schema}.{staging_events_table}
                             (artist VARCHAR,
                             auth VARCHAR,
                             first_name VARCHAR,
                             gender VARCHAR,
                             item_in_session INTEGER,
                             last_name VARCHAR,
                             length DECIMAL,
                             level VARCHAR(4) NOT NULL,
                             location TEXT,
                             method VARCHAR,
                             page VARCHAR,
                             registration DECIMAL,
                             session_id INTEGER,
                             song VARCHAR,
                             status INTEGER,
                             ts BIGINT NOT NULL,
                             user_agent TEXT,
                             user_id VARCHAR NOT NULL
                             )

                             """)

staging_songs_table_create = (f"""CREATE TABLE {songs_schema}.{staging_songs_table}
                             (num_songs INTEGER,
                             artist_id VARCHAR NOT NULL,
                             artist_latitude DECIMAL,
                             artist_longitude DECIMAL,
                             artist_location TEXT,
                             artist_name VARCHAR,
                             song_id VARCHAR NOT NULL,
                             title VARCHAR,
                             duration DECIMAL,
                             year INTEGER
                             )
                             """)

songplay_table_create = (f"""CREATE TABLE {songs_schema}.{songplay_table}
                        (songplay_id BIGINT identity(1,1) NOT NULL,
                        start_time  TIMESTAMP NOT NULL,
                        user_id VARCHAR NOT NULL,
                        level VARCHAR(4) NOT NULL,
                        song_id VARCHAR,
                        artist_id VARCHAR,
                        session_id INTEGER,
                        location TEXT,
                        user_agent TEXT,
                        
                        PRIMARY KEY (songplay_id),
                        FOREIGN KEY (user_id) REFERENCES {songs_schema}.{user_table} (user_id),
                        FOREIGN KEY (song_id) REFERENCES {songs_schema}.{song_table} (song_id),
                        FOREIGN KEY (artist_id) REFERENCES {songs_schema}.{artist_table} (artist_id),
                        FOREIGN KEY (start_time) REFERENCES {songs_schema}.{time_table} (start_time))
                        
                        distkey (songplay_id)
                        compound sortkey(user_id, song_id);
                        """)

user_table_create = (f"""CREATE TABLE {songs_schema}.{user_table}
                    (user_id VARCHAR NOT NULL,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender VARCHAR(1),
                    level VARCHAR(4) NOT NULL,
                    
                    PRIMARY KEY (user_id)
                    )
                    
                    diststyle all
                    """)

song_table_create = (f"""CREATE TABLE {songs_schema}.{song_table}
                    (song_id VARCHAR NOT NULL,
                    title VARCHAR,
                    artist_id VARCHAR NOT NULL,
                    year INTEGER,
                    duration DECIMAL,
                    
                    PRIMARY KEY (song_id),
                    FOREIGN KEY (artist_id) REFERENCES {songs_schema}.{artist_table} (artist_id)
                    )
                    
                    diststyle all
                    """)

artist_table_create = (f"""CREATE TABLE {songs_schema}.{artist_table}
                      (artist_id VARCHAR NOT NULL,
                      name VARCHAR,
                      artist_location TEXT,
                      latitude DECIMAL,
                      longitude DECIMAL,
                      
                      PRIMARY KEY (artist_id)
                      )
                    
                      diststyle all
                      """)

time_table_create = (f"""CREATE TABLE {songs_schema}.{time_table}
                    (start_time TIMESTAMP NOT NULL,
                    hour INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    week INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    weekday INTEGER NOT NULL,
                    
                    PRIMARY KEY (start_time)
                    )
                    
                    diststyle all
                    """)

# STAGING TABLES

staging_events_copy = ("""copy {}.{} from '{}' 
                      credentials 'aws_iam_role={}' 
                      region '{}'
                      json '{}';
                      """).format(songs_schema,staging_events_table, 
                                 config.get("S3","LOG_DATA"), 
                                 config.get("DWH_EXTRAS","DWH_ROLE_ARN"), 
                                 config.get("S3","REGION"),
                                 config.get("S3","LOG_JSONPATH")
                                 )

staging_songs_copy = ("""copy {}.{} from '{}' 
                      credentials 'aws_iam_role={}' 
                      region '{}'
                      json 'auto';
                      """).format(songs_schema,staging_songs_table, 
                                 config.get("S3","SONG_DATA"), 
                                 config.get("DWH_EXTRAS","DWH_ROLE_ARN"), 
                                 config.get("S3","REGION"))
# FINAL TABLES

songplay_table_insert = (f"""INSERT INTO {songs_schema}.{songplay_table}
                        (start_time, user_id, level, song_id, artist_id,
                        session_id, location, user_agent)
                        
                        
                        (select 
                        timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
                        
                        FROM 
                        {songs_schema}.{staging_events_table} events
                        
                        JOIN
                        
                        {songs_schema}.{staging_songs_table} songs
                        
                        
                        ON 
                        
                        events.song = songs.title
                        
                        AND
                        
                        events.artist = songs.artist_name)
                        
                        """)

user_table_insert = (f"""INSERT INTO {songs_schema}.{user_table}
                    
                    
                    (SELECT distinct 
                    user_id,
                    first_name,
                    last_name,
                    gender,
                    level
                    
                    FROM 
                    {songs_schema}.{staging_events_table})
                    
                    """)

song_table_insert = (f"""INSERT INTO {songs_schema}.{song_table}
                    
                    (select 
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                    
                    FROM
                    {songs_schema}.{staging_songs_table})
                    """)

artist_table_insert = (f"""INSERT INTO {songs_schema}.{artist_table}
                      
                      (select
                      artist_id,
                      artist_name,
                      artist_location,
                      artist_latitude,
                      artist_longitude
                      
                      FROM
                      {songs_schema}.{staging_songs_table})
                      """)

time_table_insert = (f"""INSERT INTO {songs_schema}.{time_table}
                    
                    (WITH time_table AS

                        (SELECT DISTINCT
                        timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time

                        FROM 

                        {songs_schema}.{staging_events_table}
                        )
                        
                        select 
                        start_time,
                        EXTRACT (hour FROM start_time) AS hour,
                        EXTRACT (day FROM start_time) AS day,
                        EXTRACT (week FROM start_time) AS week,
                        EXTRACT (month FROM start_time) AS month,
                        EXTRACT (year FROM start_time) AS year,
                        EXTRACT (dayofweek FROM start_time) AS weekday
                        
                        FROM time_table)
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  
                        user_table_create, artist_table_create, song_table_create, 
                        time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert,
                        time_table_insert, songplay_table_insert]

create_schema_queries = [songs_schema_create]

drop_schema_queries = [songs_schema_drop]
