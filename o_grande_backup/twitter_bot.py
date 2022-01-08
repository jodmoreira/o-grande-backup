import twitter_tools.main_twitter_ogb as twitter_tools
import db_tools.postgres_tools as postgres_tools
import telegram_tools.telegram_tools as telegram_tools
import aws_tools.s3_tools as s3
import tweepy
import os
from datetime import date, datetime, timedelta
import pytz
import time
from urllib3.exceptions import ProtocolError

CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
# Used to fill db entry for non agents data
NON_AGENT = os.environ.get("NON_AGENT")
NON_AGENT_ID = os.environ.get("NON_AGENT_ID")
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


class OgbListener(tweepy.Stream):
    def on_status(self, status):
        content = status._json
        now = datetime.now()
        bsb_tz = pytz.timezone("America/Sao_Paulo")
        ingestion_datetime = bsb_tz.localize(now).strftime("%Y-%m-%d %H:%M:%S%z")
        day = now.day
        if len(str(day)) == 1:
            day = f"0{now.day}"
        month = now.month
        if len(str(month)) == 1:
            month = f"0{now.month}"
        year = now.year
        post_platform_id = content["id_str"]
        if (
            content["text"].startswith("RT @") == False
            and content["in_reply_to_user_id"] == None
        ):
            print(
                f"""new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )

            screen_name = content["user"]["screen_name"]
            twitter_profile_id = postgres_tools.free_style_select(
                f"""SELECT twitter_profile_id 
                FROM twitter_profiles 
                WHERE agent_screen_name = '{screen_name}'"""
            )
            content["ogb_agent"] = True
            post_lake_dir = f"social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{screen_name}/{post_platform_id}__{now}.json"

        else:
            screen_name = NON_AGENT
            twitter_profile_id = NON_AGENT_ID
            content["ogb_agent"] = False
            post_lake_dir = f"social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{screen_name}/{post_platform_id}__{now}.json"
            print(post_lake_dir)
            print(
                f"""new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
        bucket_name = "ogb-lake"
        # s3.upload_file(bucket_name, content, post_lake_dir)
        twitter_tools.save_file_to_local_directory(
            content,
            f"{SCRIPT_PATH}/twitter_tools/temp_storage/json_data/{screen_name}-{post_platform_id}__{now}.json",
        )
        post_date = datetime.strftime(
            datetime.strptime(content["created_at"], "%a %b %d %H:%M:%S %z %Y"),
            "%Y-%m-%d %H:%M:%S",
        )
        # Will fix it later :)
        try:
            twitter_profile_id = twitter_profile_id[0]
            postgres_tools.add_new_twitter_post(
                post_platform_id,
                post_date,
                ingestion_datetime,
                post_lake_dir,
                twitter_profile_id,
            )
        except TypeError:
            telegram_tools.send_message("TypeError. Fix it!")
            pass

    def on_error(self, status_code):
        if status_code == 420:
            time.sleep(10)
            print(f"Timeout at {str(datetime.now())}")
            return True


def orchestrator(stream, users_to_follow):
    try:
        stream.filter(follow=users_to_follow, threaded=True)
    except (ProtocolError, AttributeError) as e:
        print(e)
        telegram_tools.send_message(f"OGB Twitter Stream Error, {e}")
        print("Sleep")
        time.sleep(5)
        orchestrator(stream, users_to_follow)
    except:
        telegram_tools.send_message("OGB Twitter Stream Generic Error")
        print("Sleep")
        time.sleep(5)
        orchestrator(stream, users_to_follow)


if __name__ == "__main__":
    try:
        stream = OgbListener(
            CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
        )
        profiles = postgres_tools.select_twitter_profiles()
        # Get all the twitter profiles from db and remove the non agent entry
        users_to_follow = [i[1] for i in profiles]
        users_to_follow.remove(NON_AGENT)
        orchestrator(stream, users_to_follow)
    except Exception as e:
        print(e)
        telegram_tools.send_message(f"OGB Twitter Stream Error, {e}")
