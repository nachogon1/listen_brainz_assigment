import json
import duckdb
import hashlib

def validate_json(line):
    try:
        data = json.loads(line)
        # Ensure required fields are present in track_metadata and listened_at
        if 'track_metadata' in data and 'track_metadata' in data['track_metadata']:
            track_metadata = data['track_metadata']
            if 'additional_info' in track_metadata and 'artist_msid' in track_metadata['additional_info'] and 'recording_msid' in track_metadata['additional_info']:
                return data
    except json.JSONDecodeError:
        return None  # Return None if the line is not valid JSON
    return None

def generate_listen_id(listened_at, recording_msid, user_name):
    """Generate a unique listen_id based on listened_at, recording_msid, and user_name"""
    return hashlib.sha256(f"{listened_at}-{recording_msid}-{user_name}".encode()).hexdigest()

def etl_job(input_file_path, db_connection):
    cursor = db_connection.cursor()

    # Check and create the table if not exists (already done in your code)

    with open(input_file_path, 'r') as file:
        for line in file:
            # Validate the line
            json_data = validate_json(line)
            if json_data is None:
                continue  # Skip invalid or corrupt data

            # Extract necessary information
            track_metadata = json_data['track_metadata']
            listened_at = json_data['listened_at']
            user_name = json_data['user_name']
            track_metadata_info = track_metadata['additional_info']

            artist_msid = track_metadata_info['artist_msid']
            recording_msid = track_metadata_info['recording_msid']
            release_msid = track_metadata_info['release_msid']
            release_name = track_metadata['release_name']
            track_name = track_metadata['track_name']
            artist_name = json_data['track_metadata']['artist_name']

            # Check if artist exists, insert if not
            cursor.execute("SELECT COUNT(*) FROM artists WHERE artist_msid = ?", (artist_msid,))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO artists (artist_msid, artist_name) VALUES (?, ?)", (artist_msid, artist_name))

            # Check if release exists, insert if not
            cursor.execute("SELECT COUNT(*) FROM releases WHERE release_msid = ?", (release_msid,))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO releases (release_msid, release_name) VALUES (?, ?)", (release_msid, release_name))

            # Check if track exists, insert if not
            cursor.execute("SELECT COUNT(*) FROM tracks WHERE recording_msid = ?", (recording_msid,))
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                INSERT INTO tracks (recording_msid, track_name, artist_msid, release_msid)
                VALUES (?, ?, ?, ?)
                """, (recording_msid, track_name, artist_msid, release_msid))

            # Check for duplicate listens
            listen_id = generate_listen_id(listened_at, recording_msid, user_name)
            cursor.execute("SELECT COUNT(*) FROM listens WHERE listen_id = ?", (listen_id,))
            if cursor.fetchone()[0] > 0:
                print(f"Duplicate listen: {listen_id}")
                continue  # Skip if already ingested (duplicate listen)

            # Insert into listens table
            cursor.execute("""
            INSERT INTO listens (listen_id, user_name, recording_msid, listened_at)
            VALUES (?, ?, ?, ?)
            """, (listen_id, user_name, recording_msid, listened_at))

    db_connection.commit()

def main():
    # Set up DuckDB connection
    db_connection = duckdb.connect("listen_brainz.db")
    # Run ETL job
    etl_job('./listen_brainz_assigment/database/dataset.txt', db_connection)

if __name__ == "__main__":
    main()
