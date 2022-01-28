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
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
NON_AGENT = os.environ.get("NON_AGENT")  # Used to fill db entry for non agents data
NON_AGENT_ID = os.environ.get(
    "NON_AGENT_ID"
)  # Used to fill db entry for non agents data


class OgbListener(tweepy.Stream):
    def get_profile_id(self, screen_name):
        profile_id = postgres_tools.free_style_select(
            f"""SELECT twitter_profile_id 
            FROM twitter_profiles
            WHERE agent_screen_name = '{screen_name}'"""
        )
        if profile_id == None:
            postgres_tools.add_new_agent(screen_name)
            self.get_profile_id(screen_name)
        return profile_id[0]

    def on_status(self, status):
        now = datetime.now()
        content = status._json
        post_platform_id = content["id_str"]
        if (
            content["text"].startswith("RT @") == False
            and content["in_reply_to_user_id"] == None
        ):
            print(
                f"""new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
            screen_name = content["user"]["screen_name"]
            content["ogb_agent"] = True

        else:
            screen_name = NON_AGENT
            content["ogb_agent"] = False
            print(
                f"""new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}"""
            )
        twitter_tools.save_file_to_local_directory(
            content,
            f"{SCRIPT_PATH}/twitter_tools/temp_storage/json_data/{screen_name}-{post_platform_id}__{now}.json",
        )

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
