import sqlite3


path = 'storage.db'
conn = sqlite3.connect(path)
c = conn.cursor()


def create_tb_profiles():
    try:
        c.execute(
            """CREATE TABLE profiles
            (
                screen_name TEXT,
                user_id TEXT
                )
            """
                )
    except:
        return 'error'

def add_new_profile(data):
    screen_name = (data[0],)
    query = "SELECT * FROM profiles WHERE screen_name = ?"
    c.execute(query,screen_name)
    if c.fetchone() == None:
        query = """INSERT INTO profiles (screen_name, user_id) VALUES (?,?)"""
        c.execute(query, data)
        conn.commit()
        return 'Done!'
    else:
        return 'Not updating. User already in database'

def read_profiles():
    c.execute(
        """SELECT * FROM profiles WHERE rowid > 49 ORDER BY rowid ASC""",
    )
    rows = c.fetchall()
    return rows

def create_tb_tweets_log():
    try:
        c.execute(
            """CREATE TABLE tweets_log
            (
                timestamp INTEGER,
                datetime TEXT,
                tweet_creation_datetime TEXT,
                tweet_id TEXT,
                user_id TEXT,
                screen_name TEXT
                )
            """
                    )
    except:
        return 'error'

def log_new_tweet(data):
    query = """INSERT INTO tweets_log (
    timestamp,
    datetime,
    tweet_creation_datetime,
    tweet_id,
    user_id,
    screen_name) VALUES (?,?,?,?,?,?)"""
    c.execute(query, data)
    conn.commit()
    return 'Done!'

if __name__ == '__main__':
    create_tb_tweets_log()