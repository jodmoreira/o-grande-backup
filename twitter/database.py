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
        pass

def add_new_profile(data):
    query = """INSERT INTO profiles (screen_name, user_id) VALUES (?,?)"""
    c.execute(query, data)
    conn.commit()

def read_profiles():
    c.execute(
        """SELECT * FROM profiles""",
    )
    rows = c.fetchall()
    return rows

if __name__ == '__main__':
    create_tb_profiles()