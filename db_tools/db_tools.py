import psycopg2
import os

database = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASS"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]


conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)


def new_agents_table():
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE agents (
            agent_id SERIAL PRIMARY KEY, 
            agent_name TEXT NOT NULL, 
            agent_description TEXT NOT NULL);"""
    )
    conn.commit()
    cur.close()


def new_twitter_profiles_table():
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE twitter_profiles (
            twitter_profile_id SERIAL PRIMARY KEY, 
            agent_platform_id TEXT NOT NULL,
            agent_screen_name TEXT NOT NULL,
            agent_id INTEGER REFERENCES agents(agent_id));"""
    )
    conn.commit()
    cur.close()


def new_twitter_post_table():
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE twitter_posts (
            twitter_post_id SERIAL PRIMARY KEY, 
            post_id TEXT NOT NULL, 
            post_date DATE NOT NULL, 
            post_lake_dir TEXT NOT NULL,
            twitter_profile_id INTEGER REFERENCES twitter_profiles(twitter_profile_id),
            agent_id INTEGER REFERENCES agents(agent_id));"""
    )
    conn.commit()
    cur.close()


def add_new_agent(agent_name, agent_description):
    cur = conn.cursor()

    cur.execute(
        """SELECT agent_name FROM agents WHERE agent_name = %s""", (agent_name,)
    )
    result = cur.fetchone()
    if result is None:
        cur.execute(
        """INSERT INTO agents (agent_name, agent_description)
        VALUES (%s, %s)""",
        (agent_name, agent_description),
    )
    conn.commit()
    cur.close()
    

def add_new_twitter_profile(
   agent_name, agent_twitter_screen_name, agent_platform_id):
    cur = conn.cursor()
    cur.execute(
        """SELECT 
        agent_platform_id
        FROM twitter_profiles 
        WHERE agent_platform_id = %s""", (agent_platform_id,)
    )
    result = cur.fetchone()
    if result == None:
        cur.execute(
            """INSERT INTO twitter_profiles (
            agent_platform_id,
            agent_screen_name, 
            agent_id)
            VALUES (%s, %s, (SELECT agent_id FROM agents WHERE agent_name = %s))""",
            (agent_platform_id, agent_twitter_screen_name, agent_name),
        )
    conn.commit()
    cur.close()

def add_new_twitter_post(post_id, post_date, post_lake_dir):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO twitter_posts (post_id, post_date, post_lake_dir)
        VALUES (%s, %s, %s)""",
        (post_id, post_date, post_lake_dir),
    )
    conn.commit()
    cur.close()
