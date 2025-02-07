import duckdb

# Connect to your database file
con = duckdb.connect("listen_brainz.db")

print("Task 2. a)")
# 1. Top 10 users
con.sql("""
    SELECT user_name, COUNT(*) AS listen_count
    FROM listens
    GROUP BY user_name
    ORDER BY listen_count DESC
    LIMIT 10;
""").show()

# 2. Number of users who listened on March 1, 2019:
con.sql("""
    SELECT COUNT(DISTINCT user_name) AS user_count
    FROM listens
    WHERE CAST(listened_at AS DATE) = '2019-03-01';
""").show()

# 3. First song each user listened to
con.sql("""
    SELECT user_name, recording_msid, track_name, listened_at AS first_listen
    FROM (
        SELECT l.user_name, l.recording_msid, t.track_name, l.listened_at,
               ROW_NUMBER() OVER (PARTITION BY l.user_name ORDER BY l.listened_at ASC) AS rn
        FROM listens l
        JOIN tracks t ON l.recording_msid = t.recording_msid
    ) sub
    WHERE rn = 1;
""").show()

print("Task 2. b)")

# Define and run the query
query = """
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
"""

# Execute the query and show the result
con.sql(query).show()

print("Task 2. c)")

query = """
WITH date_range AS (
    SELECT
        MIN(CAST(listened_at AS DATE)) AS min_date,
        MAX(CAST(listened_at AS DATE)) AS max_date
    FROM listens
),
     dates AS (
         -- Unnest the generated series so that each element becomes a separate row.
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
"""

# Execute the query and show the result
con.sql(query).show()