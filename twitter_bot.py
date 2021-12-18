import twitter_tools
import db_tools.db_tools as db_tools
import aws_tools.s3_tools as s3
import tweepy
import os
from datetime import date, datetime, timedelta
import pytz
import time
from urllib3.exceptions import ProtocolError

CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")


class OgbListener(tweepy.StreamListener):
    def on_status(self, status):
        content = status._json
        now = datetime.now()
        bsb_tz = pytz.timezone("America/Sao_Paulo")
        ingestion_datetime = bsb_tz.localize(now).strftime("%Y-%m-%d %H:%M:%S%z")
        print("Non-captured tweet")
        if (
            content["text"].startswith("RT @") == False
            and content["in_reply_to_user_id"] == None
        ):
            print(
                f"""new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
            s3.uploader(content)
            screen_name = content["user"]["screen_name"]
            post_platform_id = content["id_str"]
            post_date = content["created_at"]
            post_lake_dir = f"social_media/twitter/landing_zone/year={now.year}/month={now.month}/day={now.day}/{screen_name}/{content['id_str']}__{now}.json"
            agent_platform_id = content["user"]["id_str"]
            agent_id = content["user"]["id_str"]
            db_tools.add_new_twitter_post(
                post_platform_id,
                post_date,
                ingestion_datetime,
                post_lake_dir,
                agent_platform_id,
                agent_id,
            )

    def on_error(self, status_code):
        if status_code == 420:
            time.sleep(10)
            print(f"Timeout at {str(datetime.now())}")
            return True


def orchestrator(ogb_stream, users_to_follow):
    # try:
    print("listening")
    ogb_stream.filter(follow=set(users_to_follow))
    # except (ProtocolError, AttributeError) as e:
    #     print(e)
    #     print("Sleep")
    #     time.sleep(5)
    #     orchestrator(ogb_stream, users_to_follow)


if __name__ == "__main__":
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    obj_listener = OgbListener()
    ogb_stream = tweepy.Stream(auth=api.auth, listener=obj_listener)
    profiles = db_tools.select_twitter_profiles()
    users_to_follow = [i[1] for i in profiles]
    print(users_to_follow)
    orchestrator(ogb_stream, users_to_follow)
