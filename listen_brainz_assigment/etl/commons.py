import argparse
import re

# Precompiled regex for UUID validation.
UUID_REGEX = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE
)

def get_dataset_path():

    # Set up command-line argument parsing.
    parser = argparse.ArgumentParser(description="Ingest data into DuckDB.")
    parser.add_argument(
        "file_path",
        nargs="?",
        default="./listen_brainz_assigment/database/dataset.txt",
        help="Path to the dataset file (defaults to './listen_brainz_assigment/database/dataset.txt')"
    )
    args = parser.parse_args()
    dataset_path = args.file_path

    print(f"Using dataset path: {dataset_path}")
    return dataset_path