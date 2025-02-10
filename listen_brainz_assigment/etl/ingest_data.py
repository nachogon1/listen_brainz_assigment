import json
import time
import duckdb
import itertools
from datetime import datetime

from listen_brainz_assigment.database.create_db import create_tables
from listen_brainz_assigment.etl.commons import get_dataset_path
from listen_brainz_assigment.etl.ingest_data_optimized import UUID_REGEX

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


def validate_mbids(recording_mbid, release_mbid, track_mbid):
    # Validate UUID fields.
    if recording_mbid and not is_valid_uuid(recording_mbid):
        print(f"Warning: Invalid recording_mbid: {recording_mbid}")
        recording_mbid = None

    if release_mbid and not is_valid_uuid(release_mbid):
        print(f"Warning: Invalid release_mbid: {release_mbid}")
        release_mbid = None

    if track_mbid and not is_valid_uuid(track_mbid):
        print(f"Warning: Invalid track_mbid: {track_mbid}")
        track_mbid = None
    return recording_mbid, release_mbid, track_mbid


def process_batch(batch, cursor, db_connection):
    global total_records
    # Initialize accumulators for this batch.
    artists = []
    releases = []
    tracks = []
    track_tags = []
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
        tags = additional_info.get('tags', [])

        total_records += 1
        print(f"Processing record {total_records} msid {recording_msid}")
        recording_mbid, release_mbid, track_mbid = validate_mbids(recording_mbid, release_mbid, track_mbid)

        # Append to accumulators.
        artists.append((artist_msid, artist_name))
        if release_msid:
            releases.append((release_msid, release_mbid, release_name))
        tracks.append((
            recording_msid, track_name, artist_msid,
            release_msid if release_msid else None,
            recording_mbid, release_group_mbid, isrc, spotify_id, tracknumber, track_mbid
        ))
        for tag in tags:
            track_tags.append((recording_msid, tag))
        listens.append((user_name, recording_msid, listened_at))
    return artists, releases, tracks, track_tags, listens


def etl_job(input_file_path, db_connection, batch_size=100000):
    cursor = db_connection.cursor()

    with open(input_file_path, 'r') as file:
        # Process the file in batches. The iter(lambda: ...) returns [] when no more lines.
        for batch in iter(lambda: list(itertools.islice(file, batch_size)), []):
            artists, releases, tracks, track_tags, listens = process_batch(batch, cursor, db_connection)

            # Batch insert for the current batch.
            if artists:
                cursor.executemany("""
                    INSERT INTO artists (artist_msid, artist_name)
                    VALUES (?, ?)
                    ON CONFLICT DO NOTHING;
                """, artists)
            if releases:
                cursor.executemany("""
                    INSERT INTO releases (release_msid, release_mbid, release_name)
                    VALUES (?, ?, ?)
                    ON CONFLICT DO NOTHING;
                """, releases)
            if tracks:
                cursor.executemany("""
                    INSERT INTO tracks (
                        recording_msid, track_name, artist_msid, release_msid,
                        recording_mbid, release_group_mbid, isrc, spotify_id, tracknumber, track_mbid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING;
                """, tracks)
            if track_tags:
                cursor.executemany("""
                    INSERT INTO track_tags (recording_msid, tag)
                    VALUES (?, ?)
                    ON CONFLICT DO NOTHING;
                """, track_tags)
            if listens:
                cursor.executemany("""
                    INSERT INTO listens (user_name, recording_msid, listened_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT DO NOTHING;
                """, listens)

            db_connection.commit()

    print("Total records processed:", total_records)


def main():
    dataset_path = get_dataset_path()
    db_connection = duckdb.connect("listen_brainz.db")
    t0 = time.time()
    create_tables(db_connection)
    etl_job(dataset_path, db_connection, batch_size=100000)
    t1 = time.time()
    print("Time taken:", t1 - t0)


if __name__ == "__main__":
    main()
