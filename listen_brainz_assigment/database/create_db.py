import duckdb

def main():
    con = duckdb.connect("listen_brainz.db")

    con.sql('''
    CREATE TABLE IF NOT EXISTS artists (
        artist_msid UUID PRIMARY KEY,
        artist_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')

    con.sql('''
    CREATE TABLE IF NOT EXISTS releases (
        release_msid UUID PRIMARY KEY,
        release_mbid UUID,
        release_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')

    con.sql('''
    CREATE TABLE IF NOT EXISTS tracks (
        recording_msid UUID PRIMARY KEY,
        track_name TEXT NOT NULL,
        artist_msid UUID NOT NULL REFERENCES artists(artist_msid),
        release_msid UUID NOT NULL REFERENCES releases(release_msid),
        recording_mbid UUID,
        release_group_mbid UUID,
        isrc TEXT,
        spotify_id TEXT,
        tracknumber INT,
        track_mbid UUID,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')

    con.sql('''
    CREATE TABLE IF NOT EXISTS track_tags (
        recording_msid UUID NOT NULL REFERENCES tracks(recording_msid),
        tag TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (recording_msid, tag)
    );
    ''')

    con.sql('''
    CREATE TABLE IF NOT EXISTS works (
        work_mbid UUID PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')

    con.sql('''
    CREATE TABLE IF NOT EXISTS track_works (
        recording_msid UUID NOT NULL REFERENCES tracks(recording_msid),
        work_mbid UUID NOT NULL REFERENCES works(work_mbid),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (recording_msid, work_mbid)
    );
    ''')

    con.sql('''
    CREATE TABLE IF NOT EXISTS listens (
        listen_id TEXT PRIMARY KEY,
        user_name TEXT NOT NULL,
        recording_msid UUID NOT NULL REFERENCES tracks(recording_msid),
        listened_at TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (listened_at, recording_msid, user_name)
    );
    ''')

if __name__ == "__main__":
    main()

# query the table
# con.table('test').show()
