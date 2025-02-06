import json
import duckdb
import re
from datetime import datetime

UUID_REGEX = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

def is_valid_uuid(value):
    """Check if a string is a valid UUID."""
    return bool(UUID_REGEX.match(value))

def convert_unix_to_timestamp(unix_time):
    """Convert Unix timestamp (int) to DuckDB-compatible TIMESTAMP string"""
    return datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

def validate_json(line):
    """Validate and parse a JSON line from the dataset."""
    try:
        data = json.loads(line)
        if 'track_metadata' in data and 'listened_at' in data and 'user_name' in data:
            return data
    except json.JSONDecodeError:
        return None  # Skip invalid JSON
    return None

def etl_job(input_file_path, db_connection):
    cursor = db_connection.cursor()

    with open(input_file_path, 'r') as file:
        for i, line in enumerate(file):
            json_data = validate_json(line)
            if json_data is None:
                continue  # Skip invalid or corrupt data

            # Extract necessary information
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
            print(f"Ingesting record {i} msid {recording_msid}")

            # Validate UUID fields
            if recording_mbid and not is_valid_uuid(recording_mbid):
                print(f"Warning: Invalid recording_mbid found: {recording_mbid}")
                recording_mbid = None  # Set to NULL

            if release_mbid and not is_valid_uuid(release_mbid):
                print(f"Warning: Invalid release_mbid found: {release_mbid}")
                release_mbid = None  # Set to NULL

            if track_mbid and not is_valid_uuid(track_mbid):
                print(f"Warning: Invalid track_mbid found: {track_mbid}")
                track_mbid = None  # Set to NULL

            # Insert artist
            cursor.execute("""
                INSERT INTO artists (artist_msid, artist_name)
                VALUES (?, ?)
                ON CONFLICT DO NOTHING;
            """, (artist_msid, artist_name))

            # Insert release if release_msid is present
            if release_msid:
                cursor.execute("""
                    INSERT INTO releases (release_msid, release_mbid, release_name)
                    VALUES (?, ?, ?)
                    ON CONFLICT DO NOTHING;
                """, (release_msid, release_mbid, release_name))

            # Insert track (allowing NULL for invalid UUIDs)
            cursor.execute("""
                INSERT INTO tracks (
                    recording_msid, track_name, artist_msid, release_msid,
                    recording_mbid, release_group_mbid, isrc, spotify_id, tracknumber, track_mbid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING;
            """, (recording_msid, track_name, artist_msid,
                  release_msid if release_msid else None,
                  recording_mbid, release_group_mbid, isrc, spotify_id, tracknumber, track_mbid))

            # Insert tags if they exist
            for tag in tags:
                cursor.execute("""
                    INSERT INTO track_tags (recording_msid, tag)
                    VALUES (?, ?)
                    ON CONFLICT DO NOTHING;
                """, (recording_msid, tag))

            # Insert listen event
            cursor.execute("""
                INSERT INTO listens (user_name, recording_msid, listened_at)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING;
            """, (user_name, recording_msid, listened_at))

    db_connection.commit()

def main():
    db_connection = duckdb.connect("listen_brainz.db")
    etl_job('./listen_brainz_assigment/database/dataset.txt', db_connection)

if __name__ == "__main__":
    main()
