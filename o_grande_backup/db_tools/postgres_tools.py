import psycopg2
import os
import psycopg2
from sshtunnel import SSHTunnelForwarder


database = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASS"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
remote_server = os.environ["REMOTE_SERVER"]
remote_server_key = os.environ["REMOTE_SERVER_KEY"]
remote_server_user = os.environ["REMOTE_SERVER_USER"]


def connection():
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port
    )
    return conn


def new_agents_table():
    conn = connection()
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
    conn = connection()
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
    conn = connection()
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
    conn = connection()
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


def new_twitter_post_table_non_agent():
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE twitter_posts_non_agents (
            twitter_post_id SERIAL PRIMARY KEY, 
            post_platform_id TEXT NOT NULL, 
            agent_platform_id TEXT NOT NULL,
            post_date DATE NOT NULL, 
            ingestion_date DATE,
            post_lake_dir TEXT NOT NULL);"""
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


def add_new_agent(agent_name, agent_description=None):
    conn = connection()
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
    conn = connection()
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
    agent_id,
):
    conn = connection()
    cur = conn.cursor()
    sql_query = f"""INSERT INTO twitter_posts (
        post_platform_id,
        post_date,
        ingestion_date,
        post_lake_dir,
        twitter_profile_id,
        agent_id)
        VALUES ('{post_platform_id}', 
        '{post_date}', 
        '{ingestion_datetime}', 
        '{post_lake_dir}', 
        {twitter_profile_id}, 
        {agent_id})
        """
    cur.execute(
        sql_query,
    )
    conn.commit()
    cur.close()


def add_new_twitter_post_non_agent(
    post_platform_id, agent_plaform_id, post_date, ingestion_date, post_lake_dir
):
    conn = connection()
    cur = conn.cursor()
    sql_query = f"""INSERT INTO twitter_posts_non_agents (
        post_platform_id,
        agent_platform_id,
        post_date,
        ingestion_date,
        post_lake_dir)
        VALUES ('{post_platform_id}',
        '{agent_plaform_id}', 
        '{post_date}', 
        '{ingestion_date}', 
        '{post_lake_dir}')
        """
    cur.execute(
        sql_query,
    )
    conn.commit()
    cur.close()


def free_style_select(sql_query):
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        sql_query,
    )
    rows = cur.fetchone()
    cur.close()
    return rows


def select_twitter_profiles():
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT * FROM twitter_profiles""",
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_all_agents():
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT * FROM agents""",
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_all_twitter_agent_platform_id():
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT agent_platform_id FROM twitter_profiles""",
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_tweets_by_user_id(agent_id):
    conn = connection()
    cur = conn.cursor()
    sql_query = f"""SELECT twitter_profile_id FROM twitter_profiles WHERE agent_platform_id = \'{agent_id}\'"""
    cur.execute(sql_query)
    twitter_profile_id = cur.fetchone()[0]
    sql_query = f"""SELECT post_platform_id FROM twitter_posts WHERE twitter_profile_id = {twitter_profile_id}"""
    cur.execute(sql_query)
    rows = cur.fetchall()
    cur.close()
    return rows


def amount_tweets_stored_in_s3_by_date(today):
    tunnel_conn = make_the_the_tunnel()
    conn = tunnel_conn["conn"]
    server = tunnel_conn["server"]
    cur = conn.cursor()
    cur.execute(
        f"""SELECT COUNT(*) FROM twitter_posts WHERE ingestion_date = '{today}'"""
    )
    rows = cur.fetchone()
    cur.execute(
        f"""SELECT COUNT(*) FROM twitter_posts_non_agents WHERE ingestion_date = '{today}'"""
    )
    rows_non_agent = cur.fetchone()
    ## Closes the connection to the database and also the tunnel
    cur.close()
    server.close()
    return rows[0] + rows_non_agent[0]


def make_the_the_tunnel():
    """
    Creates a SSH tunnel to connect to database through remote server
    """
    server = SSHTunnelForwarder(
        (remote_server, 22),
        ssh_private_key=remote_server_key,
        ssh_username=remote_server_user,
        remote_bind_address=(host, int(port)),
    )
    server.start()
    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host="localhost",
        port=server.local_bind_port,
    )
    output = dict()
    ## Packs the tunnel and connection into a dictionary to be returned
    output["conn"] = conn
    output["server"] = server
    return output


if __name__ == "__main__":
    print(get_all_agents())
