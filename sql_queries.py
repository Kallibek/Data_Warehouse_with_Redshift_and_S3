import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

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
    CREATE TABLE IF NOT EXISTS staging_events (
                event_id       BIGINT IDENTITY(1,1),
                artist         VARCHAR,
                auth           VARCHAR,
                firstName      VARCHAR,
                gender         VARCHAR,
                itemInSession  VARCHAR,
                lastName       VARCHAR, 
                length         VARCHAR,
                level          VARCHAR,
                location       VARCHAR,
                method         VARCHAR,
                page           VARCHAR,
                registration   VARCHAR,
                sessionId      INTEGER,
                song           VARCHAR DISTKEY,
                status         INTEGER,
                ts             BIGINT SORTKEY,
                userAgent      VARCHAR,
                userId         INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER,
                artist_id           VARCHAR SORTKEY DISTKEY,
                artist_latitude     VARCHAR,
                artist_longitude    VARCHAR,
                artist_location     VARCHAR,
                artist_name         VARCHAR,
                song_id             VARCHAR,
                title               VARCHAR,
                duration            DECIMAL,
                year                INTEGER);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1) SORTKEY,
                start_time  TIMESTAMP,
                user_id     VARCHAR DISTKEY,
                level       VARCHAR,
                song_id     VARCHAR,
                artist_id   VARCHAR,
                session_id  VARCHAR,
                location    VARCHAR,
                user_agent  VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER SORTKEY DISTKEY,
                first_name  VARCHAR,
                last_name   VARCHAR,
                gender      VARCHAR,
                level       VARCHAR);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR SORTKEY,
                title       VARCHAR,
                artist_id   VARCHAR,
                year        INTEGER,
                duration    DECIMAL);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR SORTKEY,
                name        VARCHAR,
                location    VARCHAR,
                latitude    DECIMAL,
                longitude   DECIMAL)
                diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP SORTKEY DISTKEY,
                hour        INTEGER,
                day         INTEGER,
                week        INTEGER,
                month       INTEGER,
                year        INTEGER,
                weekday     INTEGER);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json '{}';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,
                            user_id, 
                            level, 
                            song_id,
                            artist_id, 
                            session_id,
                            location, 
                            user_agent)
    SELECT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second',
                        e.userId, 
                        e.level, 
                        s.song_id, 
                        s.artist_id,
                        e.sessionId, 
                        e.location,
                        e.userAgent
        FROM staging_events e
        LEFT JOIN staging_songs s
            ON e.artist = s.artist_name
            AND e.song = s.title
        WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
                        first_name,
                        last_name,
                        gender, 
                        level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE NOT (userId IS NULL OR 
                firstName IS NULL OR
                lastName IS NULL OR
                gender IS NULL OR
                level IS NULL);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,
                        title,
                        artist_id,
                        year, 
                        duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year, 
                    duration
    FROM staging_songs
    WHERE NOT (song_id IS NULL OR 
                title IS NULL OR
                artist_id IS NULL OR
                year IS NULL OR
                duration IS NULL);
""")


artist_table_insert = ("""
    INSERT INTO artists (artist_id, 
                            name, 
                            location, 
                            latitude, 
                            longitude)
    SELECT DISTINCT artist_id,
                    artist_name, 
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    WHERE NOT (artist_id IS NULL OR 
                artist_name IS NULL OR
                artist_location IS NULL OR
                artist_latitude IS NULL OR
                artist_longitude IS NULL);
""")

time_table_insert = ("""
    INSERT INTO time (start_time,
                        hour, 
                        day, 
                        week, 
                        month, 
                        year, 
                        weekday)
    SELECT DISTINCT start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(weekday FROM start_time)
    FROM songplays
    WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
