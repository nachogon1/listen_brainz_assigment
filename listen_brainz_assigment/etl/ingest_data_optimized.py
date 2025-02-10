import json
import time
import duckdb
from datetime import datetime

from listen_brainz_assigment.database.create_db import create_tables
from listen_brainz_assigment.etl.commons import get_dataset_path, UUID_REGEX

total_records = 0


def is_valid_uuid(value):
    """Return True if the value matches the UUID format."""
    return bool(UUID_REGEX.match(value))


def convert_unix_to_timestamp(unix_time):
    """Convert a Unix timestamp (int) to a DuckDB-compatible timestamp string."""
    return datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')


def validate_json(line):
    """
    Parse a JSON line and ensure it contains required keys.

    Returns the parsed data if valid, or None otherwise.
    """
    try:
        data = json.loads(line)
        if 'track_metadata' in data and 'listened_at' in data and 'user_name' in data:
            return data
    except json.JSONDecodeError:
        return None
    return None


def process_batch(batch, cursor):
    global total_records
    # Initialize accumulators for this batch.
    artists = []
    releases = []
    tracks = []
    listens = []

    for line in batch:
        json_data = validate_json(line)
        if json_data is None:
            continue  # Skip invalid/corrupt lines.

        # Extract fields from JSON.
        track_metadata = json_data['track_metadata']
        listened_at_unix = json_data['listened_at']
        listened_at = convert_unix_to_timestamp(listened_at_unix)
        user_name = json_data['user_name']
        additional_info = track_metadata.get('additional_info', {})

        artist_msid = additional_info.get('artist_msid')
        recording_msid = additional_info.get('recording_msid')
        release_msid = additional_info.get('release_msid')
        release_mbid = additional_info.get('release_mbid')
        recording_mbid = additional_info.get('recording_mbid')
        release_group_mbid = additional_info.get('release_group_mbid')
        isrc = additional_info.get('isrc')
        spotify_id = additional_info.get('spotify_id')
        tracknumber = additional_info.get('tracknumber')
        track_mbid = additional_info.get('track_mbid')

        release_name = track_metadata.get('release_name', '')
        track_name = track_metadata.get('track_name', '')
        artist_name = track_metadata.get('artist_name', '')

        total_records += 1
        print(f"Processing record {total_records} msid {recording_msid}")

        recording_mbid = recording_mbid if is_valid_uuid(recording_mbid) else None
        release_mbid = release_mbid if is_valid_uuid(release_mbid) else None
        track_mbid = track_mbid if is_valid_uuid(track_mbid) else None

        # Append to accumulators.
        artists.append((artist_msid, artist_name))
        if release_msid:
            releases.append((release_msid, release_mbid, release_name))
        tracks.append((
            recording_msid, track_name, artist_msid,
            release_msid if release_msid else None,
            recording_mbid, release_group_mbid, isrc, spotify_id, tracknumber, track_mbid
        ))
        listens.append((user_name, recording_msid, listened_at))

    return artists, releases, tracks, listens


def etl_job(dataset_path, db_connection, batch_size=100000):
    cursor = db_connection.cursor()

    # Create the raw_json table by reading the dataset
    cursor.execute(f"""
        CREATE TABLE if not exists raw_json AS
        SELECT * FROM read_json_auto('{dataset_path}', sample_size=-1, union_by_name=true, ignore_errors=true);
    """)

    # Insert artists, skipping duplicates
    # Watch out  bd23ab77-e328-414e-aac9-4bbcc8b2091c │ Thirty Seconds To Mars
    cursor.execute("""
        INSERT INTO artists (artist_msid, artist_name)
        SELECT DISTINCT on (artist_msid)
            CAST(track_metadata.additional_info.artist_msid AS UUID) AS artist_msid,
            track_metadata.artist_name AS artist_name
        FROM raw_json
        WHERE track_metadata.additional_info.artist_msid IS NOT NULL
        ON CONFLICT (artist_msid) DO NOTHING;  -- Skip duplicate
    """)

    # Insert releases, skipping duplicates
    cursor.execute("""
        INSERT INTO releases (release_msid, release_mbid, release_name)
        SELECT DISTINCT ON (release_msid)
            track_metadata.additional_info.release_msid AS release_msid,
            CASE 
                WHEN REGEXP_MATCHES(CAST(track_metadata.additional_info.release_mbid AS VARCHAR), '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') 
                THEN track_metadata.additional_info.release_mbid 
                ELSE NULL 
            END AS release_mbid,
            track_metadata.release_name AS release_name
        FROM raw_json
        WHERE track_metadata.additional_info.release_msid IS NOT NULL
        ON CONFLICT (release_msid) DO NOTHING;  -- Skip duplicate
    """)

    # Get the total number of rows to loop in batches
    cursor.execute("SELECT COUNT(*) FROM raw_json WHERE track_metadata.additional_info.recording_msid IS NOT NULL")
    total_rows = cursor.fetchone()[0]

    # Use a for loop to process the batches
    for offset in range(0, total_rows, batch_size):
        print("Processing batch starting at offset", offset, "until", offset + batch_size)
        cursor.execute(f"""
    INSERT INTO tracks (
        recording_msid, track_name, artist_msid, release_msid,
        recording_mbid, release_group_mbid, isrc, spotify_id, tracknumber, track_mbid
    )
    SELECT DISTINCT ON (recording_msid)
        CASE
            WHEN track_metadata.additional_info.recording_msid IS NOT NULL
                 AND CAST(track_metadata.additional_info.recording_msid AS VARCHAR) ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
            THEN CAST(track_metadata.additional_info.recording_msid AS UUID)
            ELSE NULL
        END AS recording_msid,
    
        track_metadata.track_name AS track_name,
        track_metadata.additional_info.artist_msid AS artist_msid,
    
        CASE
            WHEN track_metadata.additional_info.release_msid IS NOT NULL
                 AND CAST(track_metadata.additional_info.release_msid AS VARCHAR) ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
            THEN CAST(track_metadata.additional_info.release_msid AS UUID)
            ELSE NULL
        END AS release_msid,
    
        CASE
            WHEN track_metadata.additional_info.recording_mbid IS NOT NULL
                 AND CAST(track_metadata.additional_info.recording_mbid AS VARCHAR) ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
            THEN CAST(track_metadata.additional_info.recording_mbid AS UUID)
            ELSE NULL
        END AS recording_mbid,
    
        track_metadata.additional_info.release_group_mbid AS release_group_mbid,
        track_metadata.additional_info.isrc AS isrc,
        track_metadata.additional_info.spotify_id AS spotify_id,
    
        CASE
            WHEN track_metadata.additional_info.tracknumber IS NOT NULL
                 AND track_metadata.additional_info.tracknumber ~ '^[0-9]+$'
            THEN CAST(track_metadata.additional_info.tracknumber AS INT)
            ELSE NULL
        END AS tracknumber,
    
        CASE
            WHEN track_metadata.additional_info.track_mbid IS NOT NULL
                 AND CAST(track_metadata.additional_info.track_mbid AS VARCHAR) ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
            THEN CAST(track_metadata.additional_info.track_mbid AS UUID)
            ELSE NULL
        END AS track_mbid
    FROM raw_json
    WHERE track_metadata.additional_info.recording_msid IS NOT NULL
      AND CAST(track_metadata.additional_info.recording_msid AS VARCHAR) ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
    ORDER BY track_metadata.additional_info.recording_msid
    LIMIT {batch_size}
    OFFSET {offset}
    ON CONFLICT (recording_msid) DO NOTHING;
    """)
        cursor.commit()

    # Insert listens, skipping duplicates
    cursor.execute("""
        INSERT INTO listens (user_name, recording_msid, listened_at)
        SELECT 
            user_name, track_metadata.additional_info.recording_msid AS recording_msid, 
            to_timestamp(listened_at) AS listened_at
        FROM raw_json
        WHERE track_metadata.additional_info.recording_msid IS NOT NULL AND listened_at IS NOT NULL
        ON CONFLICT (listened_at, recording_msid, user_name) DO NOTHING;  -- Skip duplicate listens
    """)

    # Commit the changes
    db_connection.commit()


def main():
    dataset_path = get_dataset_path()
    db_connection = duckdb.connect("listen_brainz.db")
    # Set memory-related configurations
    db_connection.execute("SET max_memory='8GB'")
    db_connection.execute("SET memory_limit='8GB'")
    t0 = time.time()
    create_tables(db_connection)
    etl_job(dataset_path, db_connection, batch_size=10000)
    t1 = time.time()
    print(f"Time taken: {t1 - t0:.2f} seconds")


if __name__ == "__main__":
    main()
