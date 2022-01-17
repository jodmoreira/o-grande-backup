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


def new_webflow_profiles_table():
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE twitter_profiles (
            webflow_profile_id SERIAL PRIMARY KEY, 
            agent_name TEXT NOT NULL,
            agent_webflow_id TEXT NOT NULL,
            agent_id INTEGER REFERENCES agents(agent_id));"""
    )
    conn.commit()
    cur.close()


def new_twitter_post_table():
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


# def add_new_webflow_profile():
#     cur = conn.cursor()
#     cur.execute(
#         """SELECT agent_name FROM agents WHERE agent_name = %s""", (agent_name,)
#     )

#     result = cur.fetchone()
#     if result is None:
#         cur.execute(
#             """INSERT INTO agents (agent_name, agent_description)
#         VALUES (%s, %s)""",
#             (agent_name, agent_description),
#         )
#     conn.commit()
#     cur.close()


def add_new_agent(agent_name, agent_description==None):
    if agent_description == None:
        agent_description = ""
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


def add_new_twitter_profile(agent_name, agent_twitter_screen_name, agent_platform_id):
    cur = conn.cursor()
    cur.execute(
        """SELECT 
        agent_platform_id
        FROM twitter_profiles 
        WHERE agent_platform_id = %s""",
        (agent_platform_id,),
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


def add_new_twitter_post(
    post_platform_id,
    post_date,
    ingestion_datetime,
    post_lake_dir,
    twitter_profile_id,
):
    cur = conn.cursor()
    sql_query = f"""INSERT INTO twitter_posts (
        post_platform_id,
        post_date,
        ingestion_datetime,
        post_lake_dir,
        twitter_profile_id,
        agent_id)
        VALUES ('{post_platform_id}', 
        '{post_date}', 
        '{ingestion_datetime}', 
        '{post_lake_dir}', 
        {twitter_profile_id}, 
        (SELECT agent_id FROM twitter_profiles WHERE twitter_profile_id = '{twitter_profile_id}'))
        """
    cur.execute(
        sql_query,
    )
    conn.commit()
    cur.close()


def free_style_select(sql_query):
    cur = conn.cursor()
    cur.execute(
        sql_query,
    )
    rows = cur.fetchone()
    cur.close()
    return rows


def select_twitter_profiles():
    cur = conn.cursor()
    cur.execute(
        """SELECT * FROM twitter_profiles""",
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_all_agents():
    cur = conn.cursor()
    cur.execute(
        """SELECT * FROM agents""",
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_all_twitter_users_id():
    cur = conn.cursor()
    cur.execute(
        """SELECT agent_platform_id FROM twitter_profiles""",
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_tweets_by_user_id(agent_id):
    cur = conn.cursor()
    cur.execute(
        """SELECT post_platform_id FROM twitter_posts WHERE agent_id = %s""",
        (agent_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


if __name__ == "__main__":
    print(get_all_agents())
