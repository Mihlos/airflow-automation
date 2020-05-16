class SqlQueries:
    # CREATE TABLES
    staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist            TEXT,
        auth              VARCHAR(40),
        firstName         TEXT,
        gender            CHAR(12),
        itemInSession     SMALLINT,
        lastName          TEXT,
        length            DOUBLE PRECISION,
        level             CHAR(20),
        location          TEXT,
        method            CHAR(10),
        page              CHAR(20),
        registration      DOUBLE PRECISION,
        sessionId         INTEGER,
        song              TEXT,
        status            SMALLINT,
        ts                BIGINT, --TIMESTAMP throws errors
        userAgent         TEXT,
        userId            INTEGER
    )DISTSTYLE EVEN;
    """

    staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs         SMALLINT,
        artist_id         CHAR(20),
        artist_latitude   FLOAT,
        artist_longitude  FLOAT,
        artist_location   TEXT,
        artist_name       TEXT,
        song_id           CHAR(20),
        title             TEXT,
        duration          FLOAT,
        year              SMALLINT
    )DISTSTYLE EVEN;
    """)

    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id       INT IDENTITY(0,1) SORTKEY, 
        start_time        DOUBLE PRECISION NOT NULL, 
        user_id           SMALLINT NOT NULL, 
        level             VARCHAR(20) NOT NULL, 
        song_id           CHAR(20), 
        artist_id         CHAR(20), 
        session_id        CHAR(20), 
        location          TEXT, 
        user_agent        TEXT,
        PRIMARY KEY       (songplay_id)
    )DISTSTYLE EVEN;
    """)

    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id          SMALLINT SORTKEY, 
        first_name       TEXT NOT NULL, 
        last_name        TEXT NOT NULL,
        gender           TEXT, 
        level            CHAR(20),
        PRIMARY KEY      (user_id)
    )DISTSTYLE AUTO;
    """)

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (   
        song_id          CHAR(20) SORTKEY, 
        title            TEXT NOT NULL, 
        artist_id        CHAR(20), 
        year             SMALLINT, 
        duration         DOUBLE PRECISION,
        PRIMARY KEY      (song_id)
    )DISTSTYLE AUTO;
    """)

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id        CHAR(20) SORTKEY, 
        name             TEXT NOT NULL, 
        location         TEXT, 
        latitude         DOUBLE PRECISION, 
        longitude        DOUBLE PRECISION,
        PRIMARY KEY      (artist_id)
    )DISTSTYLE AUTO;
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time       BIGINT SORTKEY, 
        hour             SMALLINT, 
        day              SMALLINT, 
        week             SMALLINT, 
        month            SMALLINT, 
        year             SMALLINT, 
        weekday          SMALLINT,
        PRIMARY KEY      (start_time)
    )DISTSTYLE AUTO;
    """)
    
    # INSERT TABLES
    songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT events.ts           AS start_time,
           events.userId       AS user_id,
           level               AS level,
           songs.song_id       AS song_id,
           songs.artist_id     AS artist_id,
           events.sessionId    AS session_id,
           events.location     AS location,
           events.userAgent    AS user_agent
        FROM staging_events AS events
        JOIN staging_songs AS songs
            ON  (events.artist = songs.artist_name)
            AND (events.song = songs.title)
            AND (events.length = songs.duration)
        WHERE events.page = 'NextSong'
    """)

    user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userID) AS user_id, 
           firstName        AS first_name, 
           lastName         AS last_name, 
           gender           AS gender, 
           level            AS level
        FROM staging_events
        WHERE page = 'NextSong'
    """)

    song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, 
           title, 
           artist_id, 
           year, 
           duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id)   AS artist_id, 
           artist_name           AS name,  
           artist_location       AS location, 
           artist_latitude       AS latitude, 
           artist_longitude      AS longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  ts                                  AS start_time,
            EXTRACT(HOUR FROM t_start_time)     AS hour,
            EXTRACT(DAY FROM t_start_time)      AS day,
            EXTRACT(WEEK FROM t_start_time)     AS week,
            EXTRACT(MONTH FROM t_start_time)    AS month,
            EXTRACT(YEAR FROM t_start_time)     AS year,
            EXTRACT(DOW FROM t_start_time)      AS weekday
        FROM (
            SELECT DISTINCT(ts),
                   page,
                   '1970-01-01'::date + ts/1000 * interval '1 second' AS t_start_time
                FROM staging_events
                WHERE page = 'NextSong' 
        ) AS stg_events
    """)
