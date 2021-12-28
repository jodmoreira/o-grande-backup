## A temporary bd to hold data to tbe updated
import sqlite3

conn = sqlite3.connect("db.sqlite3")

# Create a twitter post table
def create_twitter_post_table():
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE twitter_posts (
            twitter_post_id SERIAL PRIMARY KEY, 
            post_platform_id TEXT NOT NULL, 
            post_date DATE NOT NULL, 
            ingestion_date DATE,
            post_lake_dir TEXT NOT NULL,
            twitter_profile_id INTEGER REFERENCES twitter_profiles(twitter_profile_id),
            agent_id INTEGER REFERENCES agents(agent_id));"""
    )
    conn.commit()
    cur.close()


# Writes new post to database
def add_new_twitter_post(
    post_platform_id,
    post_date,
    ingestion_date,
    post_lake_dir,
    twitter_profile_id,
    agent_id,
):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO twitter_posts (post_platform_id, post_date, ingestion_date, post_lake_dir, twitter_profile_id, agent_id)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (
            post_platform_id,
            post_date,
            ingestion_date,
            post_lake_dir,
            twitter_profile_id,
            agent_id,
        ),
    )
    conn.commit()
    cur.close()


# Reads all posts from database
def get_all_posts():
    cur = conn.cursor()
    cur.execute("""SELECT * FROM twitter_posts""")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    return result
