import duckdb
import os
import csv

# Ensure results directory exists
RESULTS_DIR = "./listen_brainz_assigment/results"


def execute_and_export(con, query, filename):
    """Execute a query, display results using DuckDB formatting, and save them as a CSV file."""

    result = con.sql(query)

    # Show results in console with DuckDB's table formatting
    result.show()

    # Fetch results for CSV writing
    rows = result.fetchall()  # Get data as a list of tuples
    columns = [desc[0] for desc in result.description]  # Extract column names

    # Save to CSV file
    csv_path = os.path.join(RESULTS_DIR, f"{filename}.csv")
    with open(csv_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(columns)  # Write headers
        writer.writerows(rows)  # Write data

    print(f"Saved results to {csv_path}")


def main():
    # Connect to DuckDB
    con = duckdb.connect("listen_brainz.db")

    print("Task 2. a)")

    # 1. Top 10 users
    execute_and_export(con, """
        SELECT user_name, COUNT(*) AS listen_count
        FROM listens
        GROUP BY user_name
        ORDER BY listen_count DESC
        LIMIT 10;
    """, "top_10_users")

    # 2. Users who listened on March 1, 2019
    execute_and_export(con, """
        SELECT COUNT(DISTINCT user_name) AS user_count
        FROM listens
        WHERE CAST(listened_at AS DATE) = '2019-03-01';
    """, "users_march_1_2019")

    # 3. First song each user listened to
    execute_and_export(con, """
        SELECT user_name, recording_msid, track_name, listened_at AS first_listen
        FROM (
            SELECT l.user_name, l.recording_msid, t.track_name, l.listened_at,
                   ROW_NUMBER() OVER (PARTITION BY l.user_name ORDER BY l.listened_at ASC) AS rn
            FROM listens l
            JOIN tracks t ON l.recording_msid = t.recording_msid
        ) sub
        WHERE rn = 1;
    """, "first_song_per_user")

    print("Task 2. b)")

    # 4. Top 3 days for each user by number of listens
    execute_and_export(con, """
        WITH daily_listens AS (
            SELECT 
                user_name,
                CAST(listened_at AS DATE) AS date,
                COUNT(*) AS number_of_listens
            FROM listens
            GROUP BY user_name, CAST(listened_at AS DATE)
        ),
        ranked_listens AS (
            SELECT 
                user_name,
                date,
                number_of_listens,
                ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY number_of_listens DESC, date ASC) AS rn
            FROM daily_listens
        )
        SELECT 
            user_name AS user,
            number_of_listens,
            date
        FROM ranked_listens
        WHERE rn <= 3
        ORDER BY user, number_of_listens DESC;
    """, "top_3_days_per_user")

    print("Task 2. c)")

    # 5. Active users per day
    execute_and_export(con, """
        WITH date_range AS (
            SELECT
                MIN(CAST(listened_at AS DATE)) AS min_date,
                MAX(CAST(listened_at AS DATE)) AS max_date
            FROM listens
        ),
        dates AS (
            SELECT t.date
            FROM date_range,
                 UNNEST(generate_series(min_date, max_date, INTERVAL '1 day')) AS t(date)
        ),
        active_users AS (
            SELECT
                d.date,
                COUNT(DISTINCT l.user_name) AS number_active_users
            FROM dates d
            LEFT JOIN listens l
            ON CAST(l.listened_at AS DATE) BETWEEN d.date - INTERVAL '6 days' AND d.date
            GROUP BY d.date
        )
        SELECT
            date,
            number_active_users,
            (number_active_users * 100.0) / (SELECT COUNT(DISTINCT user_name) FROM listens) AS percentage_active_users
        FROM active_users
        ORDER BY date;
    """, "daily_active_users")


if __name__ == "__main__":
    main()
