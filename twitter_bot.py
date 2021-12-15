import twitter_tools
import db_tools
import aws_tools

import tweepy
import os
import s3
import database
from datetime import datetime, timedelta
import time
from urllib3.exceptions import ProtocolError

CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")


class OgbListener(tweepy.StreamListener):
    def on_status(self, status):
        content = status._json
        if (
            content["text"].startswith("RT @") == False
            and content["in_reply_to_user_id"] == None
        ):
            # Just to adjust to my current timezone
            print(
                f"""new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
            s3.uploader(content)
            database_payload = (
                content["user"]["screen_name"],
                content["user"]["id_str"],
                content["id_str"],
                content["created_at"],
                str(datetime.now()),
                int(time.time()),
            )
            database.log_new_tweet(database_payload)

    def on_error(self, status_code):
        if status_code == 420:
            time.sleep(10)
            print(f"Timeout at {str(datetime.now())}")
            return True


def orchestrator(ogb_stream, users_to_follow):
    try:
        ogb_stream.filter(follow=set(users_to_follow))
    except (ProtocolError, AttributeError) as e:
        print(e)
        print("Sleep")
        time.sleep(5)
        main(ogb_stream, users_to_follow)


def main(ogb_stream, users_to_follow):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    obj_listener = OgbListener()
    ogb_stream = tweepy.Stream(auth=api.auth, listener=obj_listener)
    profiles = database.read_profiles()
    users_to_follow = [i[1] for i in profiles]
    main(ogb_stream, users_to_follow)
