import tweepy
from tweepy import API
from tweepy import OAuthHandler
import time
import json
import os
from datetime import datetime, timedelta
import pytz

CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
NON_AGENT = os.environ.get("NON_AGENT")
NON_AGENT_ID = os.environ.get("NON_AGENT_ID")

# auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
# api = API(auth)


class Twitter_history_posts:
    ##### For the first execution or recoverying from failures #####
    def __init__(self, user_id=None, screen_name=None):
        self.auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.api = API(self.auth)
        self.screen_name = screen_name
        if screen_name != None:
            self.user_id = self.get_user_id()
        else:
            self.user_id = user_id

    def get_last_tweets(self, delay_time=None):
        def requester(max_id=None, output=None):
            if output == None:
                output = []
            tweets = self.api.user_timeline(
                user_id=self.user_id,
                tweet_mode="extended",
                count=200,
                max_id=max_id,
            )
            if len(tweets) > 0:
                for tweet in tweets:
                    id_tweet = tweet.id
                    # print(f"{id_tweet} from {self.user_id}")
                    output.append(tweet._json)
                    max_id = id_tweet - 1
                time.sleep(delay_time)
                requester(max_id, output)
            else:
                print("No more tweets")
            return output

        return requester()

    def get_user_id(self):
        id = self.api.get_user(screen_name=self.screen_name).id
        print(id)
        return id

    def lake_path_creator(self, tweet):
        """
        Creates the lake path string to be used when uploading a new tweet without the streaming API.
        """
        now = datetime.now()
        if (
            tweet["full_text"].startswith("RT @") == False
            and tweet["in_reply_to_user_id"] == None
        ):
            screen_name = tweet["user"]["screen_name"]
            post_lake_dir = f"social_media/twitter/landing_zone/year={now.year}/month={now.month}/day={now.day}/{screen_name}/{tweet['id_str']}__{now}.json"
        else:
            screen_name = NON_AGENT
            post_lake_dir = f"social_media/twitter/landing_zone/year={now.year}/month={now.month}/day={now.day}/{screen_name}/{tweet['id_str']}__{now}.json"
        return post_lake_dir

    def add_new_tweet_to_db(self, tweet, post_lake_dir):
        """
        Adds a new tweet to the database
        """
        now = datetime.now()
        bsb_tz = pytz.timezone("America/Sao_Paulo")
        ingestion_datetime = bsb_tz.localize(now).strftime("%Y-%m-%d %H:%M:%S%z")
        post_platform_id = tweet["id_str"]
        if (
            tweet["text"].startswith("RT @") == False
            and tweet["in_reply_to_user_id"] == None
        ):
            print(
                f"""new tweet from {tweet["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
            # Gets agent screen_name from the tweet
            screen_name = tweet["user"]["screen_name"]
            # Finds the agent_twitter_id
            twitter_profile_id = postgres_tools.free_style_select(
                f"""SELECT twitter_profile_id 
                FROM twitter_profiles 
                WHERE agent_screen_name = '{screen_name}'"""
            )
        else:
            # As is is not a tweet made by an agent, a default screen and profile id is used
            screen_name = NON_AGENT
            twitter_profile_id = NON_AGENT_ID
            print(
                f"""new tweet from {tweet["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
        post_date = datetime.strftime(
            datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y"),
            "%Y-%m-%d %H:%M:%S",
        )
        twitter_profile_id = twitter_profile_id[0]
        postgres_tools.add_new_twitter_post(
            post_platform_id,
            post_date,
            ingestion_datetime,
            post_lake_dir,
            twitter_profile_id,
        )


def save_file_to_local_directory(tweet, path):
    """
    Saves the tweet to a local directory
    """
    with open(path, "w") as f:
        json.dump(tweet, f)


## Legacy functions
def writer(tweet, user):
    try:
        diretorio_escrita = fr"{FILES_DIR}{user}"
        doc_saida = open(diretorio_escrita, "a+", encoding="utf8")
    except FileNotFoundError:
        diretorio_escrita = f"{FILES_DIR}{user}"
        doc_saida = open(diretorio_escrita, "a+", encoding="utf8")
    tweet = json.dumps(tweet._json, ensure_ascii=False)
    doc_saida.write(tweet)
    doc_saida.write("\n")
    doc_saida.close()


def get_greater_id(user):
    try:
        doc = open(fr"{DIRETORIO_ARQUIVOS}\{user}", "r", encoding="utf8")
    except FileNotFoundError:
        doc = open(fr"{DIRETORIO_ARQUIVOS}/{user}", "r", encoding="utf8")
    doc = doc.readlines()
    doc_json = [json.loads(i) for i in doc]
    ids = [i["id"] for i in doc_json]
    maior_id = max(ids)
    maior_id = maior_id + 1
    return maior_id


def run_since_id(user):
    maior_id = get_greater_id(user)
    tweets = api.user_timeline(
        id=user, tweet_mode="extended", count=200, since_id=maior_id
    )
    if len(tweets) < 1:
        print(f"nothing found from {user}")
        return
    for tweet in tweets:
        id_tweet = tweet.id
        print(f"{id_tweet} from {user}")
        writer(tweet, user)
    try:
        run_since_id(user)
        print("New item")
    except Exception as e:
        print(e)
        print("sleep")
        time.sleep(15 * 60)
        run_since_id(user)


# if __name__ == "__main__":
# auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
# api = API(auth)
# print(user_id)
# obj = Twitter_history_posts(screen_name="????")
# obj.test()
