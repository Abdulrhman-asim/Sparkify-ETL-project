import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS \"user\";"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS \"time\";"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events(
    artist              VARCHAR(65535),
    auth                VARCHAR(25)   NOT NULL,
    firstName           TEXT,
    gender              VARCHAR(1),
    itemInSession       INTEGER,
    lastName            TEXT,
    length              DECIMAL,
    level               VARCHAR(10)   NOT NULL,
    location            VARCHAR(65535),
    method              VARCHAR(10)   NOT NULL,
    page                TEXT          NOT NULL,
    registration        DECIMAL,
    sessionId           INTEGER       NOT NULL,
    song                VARCHAR(65535),
    status              INTEGER       NOT NULL,
    ts                  BIGINT        NOT NULL,
    userAgent           TEXT,
    userId              INTEGER
    );
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs           INTEGER         NOT NULL,
    artist_id           VARCHAR(25)     NOT NULL,
    artist_latitude     DECIMAL,
    artist_longitude    DECIMAL,
    artist_location     VARCHAR(65535),
    artist_name         VARCHAR(65535)  NOT NULL,
    song_id             VARCHAR(25)     NOT NULL,
    title               VARCHAR(65535)  NOT NULL,
    duration            DECIMAL         NOT NULL,
    year                INTEGER         NOT NULL
    );
""")

songplay_table_create = ("""
CREATE TABLE songplay(
    songplay_id INTEGER         IDENTITY(0,1) PRIMARY KEY,
    start_time  BIGINT          NOT NULL REFERENCES "time"(start_time), 
    user_id     INTEGER         NOT NULL REFERENCES "user"(user_id),
    level       VARCHAR(10)     NOT NULL,
    song_id     VARCHAR(25)     NOT NULL REFERENCES song(song_id),
    artist_id   VARCHAR(25)     NOT NULL REFERENCES artist(artist_id),
    session_id  INTEGER         NOT NULL,
    location    VARCHAR(65535),
    user_agent  TEXT            NOT NULL
    );
""")

user_table_create = ("""
CREATE TABLE "user"(
    user_id     INTEGER      PRIMARY KEY,
    first_name  TEXT         NOT NULL,
    last_name   TEXT         NOT NULL,
    gender      VARCHAR(1),
    level       VARCHAR(10)  NOT NULL
    );
""")

song_table_create = ("""
CREATE TABLE song(
    song_id     VARCHAR(20)           PRIMARY KEY,
    title       VARCHAR(65535)        NOT NULL,
    artist_id   VARCHAR(25)           NOT NULL,
    year        INTEGER               NOT NULL,
    duration    DECIMAL               NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE artist(
    artist_id           VARCHAR(25)           PRIMARY KEY,
    name                VARCHAR(65535)        NOT NULL,
    location            VARCHAR(65535),
    artist_latitude     DECIMAL,
    artist_longitude    DECIMAL
    );
""")

time_table_create = ("""
CREATE TABLE "time"(
    start_time BIGINT   PRIMARY KEY,
    hour       SMALLINT NOT NULL,
    day        SMALLINT NOT NULL,
    week       SMALLINT NOT NULL,
    month      SMALLINT NOT NULL,
    year       SMALLINT NOT NULL,
    weekday    SMALLINT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
JSON {}
IAM_ROLE '{}'
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'],
            config['S3']['LOG_JSONPATH'],
            config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
JSON 'auto'
IAM_ROLE '{}'
REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id ,session_id ,user_agent)
SELECT DISTINCT
    se.ts           AS start_time, 
    se.userId       AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId    AS session_id,
    se.userAgent    AS user_agent
FROM
    staging_events se JOIN staging_songs ss ON se.song = ss.title
WHERE 
    se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO "user" (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId       AS user_id,
    firstName    AS first_name,
    lastName     AS last_name,
    gender,
    level
FROM
    staging_events
WHERE
    page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM 
    staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, artist_latitude, artist_longitude)
SELECT DISTINCT
    artist_id,
    artist_name     AS name,
    artist_location AS location,
    artist_latitude,
    artist_longitude
FROM
    staging_songs
""")

time_table_insert = ("""
INSERT INTO "time" (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    ts                                                                              AS start_time,
    EXTRACT(HOUR FROM TIMESTAMP 'epoch' + (ts / 1000.0) * INTERVAL '1 second')      AS hour,
    EXTRACT(DAY FROM TIMESTAMP 'epoch' + (ts / 1000.0) * INTERVAL '1 second')       AS day,
    EXTRACT(WEEK FROM TIMESTAMP 'epoch' + (ts / 1000.0) * INTERVAL '1 second')      AS week,
    EXTRACT(MONTH FROM TIMESTAMP 'epoch' + (ts / 1000.0) * INTERVAL '1 second')     AS month,
    EXTRACT(YEAR FROM TIMESTAMP 'epoch' + (ts / 1000.0) * INTERVAL '1 second')      AS year,
    EXTRACT(DOW FROM TIMESTAMP 'epoch' + (ts / 1000.0) * INTERVAL '1 second')       AS weekday
FROM
    staging_events
WHERE
    page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, staging_events_table_drop, staging_songs_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
