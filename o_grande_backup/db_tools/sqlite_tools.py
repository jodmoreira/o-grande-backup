## A temporary bd to hold data to tbe updated
import sqlite3
import os


# # Create a twitter post table
# def create_twitter_post_table():
#     conn = sqlite3.connect("db.sqlite3")
#     cur = conn.cursor()
#     cur.execute(
#         """CREATE TABLE twitter_posts (
#             twitter_post_id SERIAL PRIMARY KEY,
#             post_platform_id TEXT NOT NULL,
#             post_date DATE NOT NULL,
#             ingestion_date DATE,
#             post_lake_dir TEXT NOT NULL,
#             twitter_profile_id INTEGER REFERENCES twitter_profiles(twitter_profile_id),
#             agent_id INTEGER REFERENCES agents(agent_id));"""
#     )
#     conn.commit()
#     cur.close()


# # Writes new post to database
# def add_new_twitter_post(
#     conn = sqlite3.connect("db.sqlite3")
#     post_platform_id,
#     post_date,
#     ingestion_date,
#     post_lake_dir,
#     twitter_profile_id,
#     agent_id,
# ):
#     cur = conn.cursor()
#     cur.execute(
#         """INSERT INTO twitter_posts (post_platform_id, post_date, ingestion_date, post_lake_dir, twitter_profile_id, agent_id)
#         VALUES (?, ?, ?, ?, ?, ?)""",
#         (
#             post_platform_id,
#             post_date,
#             ingestion_date,
#             post_lake_dir,
#             twitter_profile_id,
#             agent_id,
#         ),
#     )
#     conn.commit()
#     cur.close()


# # Reads all posts from database
# def get_all_posts():
#     cur = conn.cursor()
#     cur.execute("""SELECT * FROM twitter_posts""")
#     result = cur.fetchall()
#     conn.commit()
#     cur.close()
#     return result


def stand_alone_connection(directory):
    sharing_zone_directory = os.environ.get("SHARING_ZONE_DIRECTORY")
    conn = sqlite3.connect(directory)
    return conn


def spreadsheet_table():
    sharing_zone_directory = os.environ.get("SHARING_ZONE_DIRECTORY")
    conn = sqlite3.connect(
        f"{sharing_zone_directory}/social_media/spreadsheets_logs/twitter_logs.db"
    )
    return conn


def create_spreadsheet_table():
    conn = spreadsheet_table()
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE spreadsheets (
        spreadsheet_name TEXT NOT NULL,
        spreadsheet_url TEXT NOT NULL,
        spreadsheet_folder_id TEXT NOT NULL)"""
    )
    conn.commit()


def add_new_spreadsheet(spreadsheet_name, spreadsheet_url, spreadsheet_folder_id):
    conn = spreadsheet_table()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO spreadsheets (spreadsheet_name, spreadsheet_url, spreadsheet_folder_id)
            VALUES (?, ?, ?)""",
            (spreadsheet_name, spreadsheet_url, spreadsheet_folder_id),
        )
    except sqlite3.OperationalError:
        create_spreadsheet_table()
        add_new_spreadsheet(spreadsheet_name, spreadsheet_url, spreadsheet_folder_id)
    conn.commit()


def create_spreadsheet_post_table():
    conn = spreadsheet_table()
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE spreadsheet_posts (
        spreadsheet_id INTEGER REFERENCES spreadsheets(spreadsheet_id),
        post_id INTEGER NOT NULL,
        post_created_at DATE NOT NULL,
        spreadsheet_write_date DATE NOT NULL,
        agent_name TEXT NOT NULL)"""
    )
    conn.commit()


def insert_new_post(
    spreadsheet_id, post_id, post_created_at, spreadsheet_write_date, agent_name
):
    conn = spreadsheet_table()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""INSERT INTO spreadsheet_posts (spreadsheet_id, post_id, post_created_at, spreadsheet_write_date, agent_name)
            VALUES (?, ?, ?, ?, ?)""",
            (
                spreadsheet_id,
                post_id,
                post_created_at,
                spreadsheet_write_date,
                agent_name,
            ),
        )
    except sqlite3.OperationalError:
        create_spreadsheet_post_table()
        insert_new_post(
            spreadsheet_id, post_id, post_created_at, spreadsheet_write_date, agent_name
        )
    conn.commit()


def read_twitter_posts(agent_name):
    """Check all tweets from a specific agent"""
    conn = spreadsheet_table()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""SELECT post_date FROM spreadsheet_posts WHERE agent_name = {agent_name}"""
        )
        result = cur.fetchall()
    except sqlite3.OperationalError:
        create_spreadsheet_post_table()
        read_twitter_posts(agent_name)
    conn.commit()
    cur.close()
    return result


def amount_tweets_stored_in_sheets_by_date(date):
    conn = spreadsheet_table()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""SELECT COUNT(*) FROM spreadsheet_posts WHERE strftime('%Y-%m-%d', spreadsheet_write_date) = '{date}'"""
        )
        result = cur.fetchall()
    except sqlite3.OperationalError:
        create_spreadsheet_post_table()
        amount_tweets_stored_in_sheets_by_date(date)
    conn.commit()
    cur.close()
    return result[0][0]


def get_spreadsheet_id_from_twitter_logs(agent_name):
    conn = spreadsheet_table()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""SELECT spreadsheet_url FROM spreadsheets WHERE spreadsheet_name = '{agent_name}'"""
        )
        result = cur.fetchall()
    except sqlite3.OperationalError:
        get_spreadsheet_id(agent_name)
    conn.commit()
    cur.close()
    result = result[0][0].split("/")[5]
    return result


if __name__ == "__main__":
    try:
        create_spreadsheet_table()
    except sqlite3.OperationalError:
        pass
    try:
        create_spreadsheet_post_table()
    except sqlite3.OperationalError:
        pass
