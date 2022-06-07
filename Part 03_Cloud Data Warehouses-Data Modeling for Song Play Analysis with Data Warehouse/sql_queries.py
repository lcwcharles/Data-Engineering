import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get('S3','lOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
REGION=config.get('S3','REGION')
ARN=config.get('IAM_ROLE','ARN')


# DROP TABLES

staging_events_table_drop = "DROP  TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP  TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP  TABLE IF EXISTS songplays;"
user_table_drop = "DROP  TABLE IF EXISTS users;"
song_table_drop = "DROP  TABLE IF EXISTS songs;"
artist_table_drop = "DROP  TABLE IF EXISTS artists;"
time_table_drop = "DROP  TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= """
                              CREATE  TABLE IF NOT EXISTS staging_events
                              (
                                  artist_name VARCHAR,
                                  auth VARCHAR(20),
                                  user_firstName VARCHAR(20),
                                  user_gender VARCHAR(4),
                                  song_itemInSession INT,
                                  user_lastName VARCHAR(20),
                                  song_length NUMERIC,
                                  user_level VARCHAR,
                                  user_location VARCHAR,
                                  method VARCHAR,
                                  page VARCHAR,
                                  registration BIGINT,
                                  session_id INT ,
                                  song_title VARCHAR,
                                  status INT,
                                  ts BIGINT,
                                  user_agent VARCHAR,
                                  user_id INT
                              );
                             """

staging_songs_table_create1 = """
                              CREATE  TABLE IF NOT EXISTS staging_songs
                              (
                                  num_songs INT,
                                  artist_id VARCHAR,
                                  artist_latitude NUMERIC,
                                  artist_longitude NUMERIC,
                                  artist_location VARCHAR,
                                  artist_name VARCHAR,
                                  song_id VARCHAR,
                                  song_title VARCHAR,
                                  song_duration NUMERIC,
                                  song_year INT
                              );
                             """
staging_songs_table_create ="""CREATE TABLE IF NOT EXISTS staging_songs (
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
                                """

user_table_create = """
                      CREATE  TABLE IF NOT EXISTS users
                      (
                          user_key VARCHAR PRIMARY KEY sortkey,
                          user_id VARCHAR,
                          user_firstname VARCHAR,
                          user_lastname VARCHAR,
                          user_gender VARCHAR,
                          user_level VARCHAR
                      );
                    """

song_table_create = """
                      CREATE  TABLE IF NOT EXISTS songs
                      (
                          song_key VARCHAR PRIMARY KEY sortkey, 
                          song_id VARCHAR,
                          song_title VARCHAR,
                          artist_id VARCHAR,
                          song_year INT,
                          song_duration NUMERIC
                      );
                    """

artist_table_create = """
                      CREATE  TABLE IF NOT EXISTS artists
                      (
                          artist_key VARCHAR PRIMARY KEY sortkey,
                          artist_id VARCHAR,
                          artist_name VARCHAR,
                          artist_location VARCHAR,
                          artist_lattitude NUMERIC,
                          artist_longitude NUMERIC
                      );
                      """

time_table_create = """
                      CREATE  TABLE IF NOT EXISTS time
                      (
                          time_key TIMESTAMP PRIMARY KEY sortkey,
                          start_time TIMESTAMP,
                          hour INT,
                          day INT,
                          week INT,
                          month INT,
                          year INT,
                          weekday INT
                      );
                    """

songplay_table_create = """
                        CREATE  TABLE IF NOT EXISTS songplays
                        (
                            songplay_key INT IDENTITY(0,1) PRIMARY KEY distkey,
                            time_key TIMESTAMP REFERENCES time(time_key),
                            user_key VARCHAR REFERENCES users(user_key),
                            user_level VARCHAR,
                            song_key VARCHAR REFERENCES songs(song_key),
                            artist_key VARCHAR REFERENCES artists(artist_key),
                            session_id INT,
                            user_location VARCHAR,
                            user_agent VARCHAR         
                        );
                        """

# STAGING TABLES

staging_events_copy = ("""copy staging_events
                          from {}
                          credentials 'aws_iam_role={}'
                          json as {}
                          region {}  timeformat as 'epochmillisecs';
                       """).format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""copy staging_songs
                          from {}
                          credentials 'aws_iam_role={}'
                          json as 'auto'
                          region {} ;
                       """).format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays ( time_key, user_key, user_level, song_key, 
                                                artist_key, session_id, user_location, user_agent)
                        SELECT timestamp without time zone 'epoch' + se.ts/1000 * interval '1 second'    AS time_key, 
                                se.user_id     AS user_key, 
                                se.user_level, 
                                ss.song_id     AS song_key,
                                ss.artist_id   AS artist_key, 
                                se.session_id, 
                                se.user_location, 
                                se.user_agent
                        FROM staging_events se
                        JOIN staging_songs ss
                        ON se.artist_name = ss.artist_name and se.song_title = ss.title and se.song_length = ss.duration
                        WHERE se.page='NextSong'
                        
                        """)

user_table_insert = ("""
                    INSERT INTO users (user_key, user_id, user_firstname, user_lastname, user_gender, user_level)
                    SELECT distinct(user_id) AS user_key, 
                           user_id, 
                           user_firstname, 
                           user_lastname, 
                           user_gender, 
                           user_level
                    FROM staging_events where page='NextSong'
                    """)

song_table_insert = ("""
                    INSERT INTO songs (song_key, song_id, song_title, artist_id, song_year, song_duration)
                    SELECT distinct(song_id) AS song_key,
                           song_id,
                           title,
                           artist_id,
                           year,
                           duration
                    FROM staging_songs
                    """)

artist_table_insert = ("""
                       INSERT INTO artists (artist_key, artist_id, artist_name, artist_location, artist_lattitude, artist_longitude) 
                       SELECT distinct(artist_id) AS artist_key,
                              artist_id,
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longitude
                       FROM staging_songs
                       """)

time_table_insert = ("""
                    INSERT INTO time (time_key, start_time, hour, day, week, month, year, weekday)
                    SELECT DISTINCT timestamp without time zone 'epoch' + ts/1000 * interval '1 second' as time_key, 
                           timestamp without time zone 'epoch' + ts/1000 * interval '1 second' as start_time, 
                           EXTRACT(hour from start_time) AS hour, 
                           EXTRACT(day from start_time) AS day, 
                           EXTRACT(week from start_time) AS week, 
                           EXTRACT(month from start_time) AS month, 
                           EXTRACT(year from start_time) AS year, 
                           EXTRACT(weekday from start_time) AS weekday
                    FROM staging_events where page='NextSong'
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, user_table_create, 
                        song_table_create, artist_table_create,  time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
