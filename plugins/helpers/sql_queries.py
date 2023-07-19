class SqlQueries:
    # CREATE TABLES
    staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS log_data (
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
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId BIGINT);""")

    staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS song_data (
    artist_id   VARCHAR NOT NULL,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    duration FLOAT,
    year INT);""")

    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongPlays (
        songplay_id  BIGINT IDENTITY(1,1) PRIMARY KEY,
        start_time   BIGINT NOT NULL REFERENCES dimTime(start_time),
        user_key     BIGINT NOT NULL REFERENCES dimUsers(user_key),
        user_id      BIGINT,
        level        VARCHAR NOT NULL,
        song_id      VARCHAR NOT NULL REFERENCES dimSongs(song_id),
        artist_id    VARCHAR NOT NULL REFERENCES dimArtists(artist_id),
        session_id   INT NOT NULL,
        location     VARCHAR,
        user_agent   TEXT NOT NULL);""")

    user_table_delete =("""
    DROP TABLE IF EXISTS dimUsers;
    """)

    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUsers (
        user_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
        user_id    BIGINT,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     VARCHAR,
        level      VARCHAR NOT NULL);""")

    song_table_delete =("""
    DROP TABLE IF EXISTS dimSongs;
    """)

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimSongs (
        song_id   VARCHAR NOT NULL PRIMARY KEY,
        title     VARCHAR,
        artist_id VARCHAR NOT NULL REFERENCES dimArtists(artist_id),
        year      INT NOT NULL,
        duration  INT NOT NULL);""")

    artist_table_delete =("""
    DROP TABLE IF EXISTS dimArtists;
    """)

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimArtists (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name      VARCHAR,
        location  VARCHAR,
        latitude  FLOAT,
        longitude FLOAT);""")

    time_table_delete =("""
    DROP TABLE IF EXISTS dimTime;
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimTime (
        start_time BIGINT NOT NULL PRIMARY KEY,
        hour       INT NOT NULL,
        day        INT NOT NULL,
        week       INT NOT NULL,
        month      INT NOT NULL,
        year       INT NOT NULL,
        weekday    INT NOT NULL);""")




    # songplay_table_insert = ("""
    #     INSERT INTO factSongPlays (song_id, artist_id,
    #                         session_id,user_id, start_time,
    #                         level, location, user_agent, user_key)
    #     SELECT
    #             md5(events.sessionid || events.start_time) songplay_id,
    #             events.start_time, 
    #             events.userid, 
    #             events.level, 
    #             songs.song_id, 
    #             songs.artist_id, 
    #             events.sessionid, 
    #             events.location, 
    #             events.useragent
    #             FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
    #         FROM log_data
    #         WHERE page='NextSong') events
    #         LEFT JOIN song_data songs
    #         ON events.song = songs.title
    #             AND events.artist = songs.artist_name
    #             AND events.length = songs.duration
    # """)

    # songplay_table_insert = ("""
    #     INSERT INTO factSongPlays (
    #         start_time,
    #         user_id,
    #         level,
    #         song_id,
    #         artist_id,
    #         session_id,
    #         location,
    #         user_agent
    #     )
    #     SELECT
    #         CAST(events.userid AS BIGINT) AS user_id,
    #         CAST(events.ts AS BIGINT) as start_time,
    #         events.level,
    #         songs.song_id,
    #         songs.artist_id,
    #         events.sessionid,
    #         events.location,
    #         events.useragent
    #     FROM log_data events
    #     LEFT JOIN song_data songs
    #     ON events.song = songs.title
    #         AND events.artist = songs.artist_name
    #         AND events.length = songs.duration
    #     WHERE events.page = 'NextSong'
    # """)

    songplay_table_insert = ("""
        INSERT INTO factSongPlays (song_id, artist_id,
                                    session_id,user_id, start_time,
                                    level, location, user_agent, user_key)
        SELECT sd.song_id AS song_id,
                sd.artist_id AS artist_id,
                ld.sessionId AS session_id,
                ld.userId AS user_id,
                ld.ts AS start_time,
                ld.level AS level,
                ld.location AS location,
                ld.userAgent AS user_agent,
                du.user_key AS user_key
        FROM log_data ld
        JOIN song_data sd ON
        LOWER(REGEXP_REPLACE(ld.song, '[^a-zA-Z0-9]', '')) = LOWER(REGEXP_REPLACE(sd.title, '[^a-zA-Z0-9]', ''))
        AND LOWER(REGEXP_REPLACE(ld.artist, '[^a-zA-Z0-9]', '')) = LOWER(REGEXP_REPLACE(sd.artist_name, '[^a-zA-Z0-9]', ''))
        JOIN dimUsers du ON ld.userId = du.user_id;""")

    user_table_insert = ("""
        INSERT INTO dimUsers (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender AS gender,
                        level AS level
        FROM log_data;""")

    song_table_insert = ("""
        INSERT INTO dimSongs (song_id,  title, artist_id, year, duration)
        SELECT DISTINCT song_id AS song_id,
                        title AS title,
                        artist_id AS artist_id,
                        year AS year,
                        duration AS duration
        FROM song_data;""")

    artist_table_insert = ("""
        INSERT INTO dimArtists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id AS artist_id,
                        artist_name AS name,
                        artist_location AS location,
                        artist_latitude AS latitude,
                        artist_longitude AS longitude
        FROM song_data;""")

    time_table_insert = ("""
        INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT ts AS start_time,
        EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')::INT AS hour,
        EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')::INT AS day,
        EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')::INT AS week,
        EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')::INT AS month,
        EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')::INT AS year,
        EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')::INT AS weekday
        FROM log_data;""")