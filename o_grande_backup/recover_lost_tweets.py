## Script to get tweets lost for some reason
import db_tools.postgres_tools as postgres_tools
import twitter_tools.main_twitter_ogb as twitter_tools
from datetime import datetime, timedelta
import time
import os

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
NON_AGENT = os.environ.get("NON_AGENT")


def recover_lost_tweets():
    """Get the last ~3200 tweets from each agent from the API and
    upload to S3. It is supose to be used only to get missing data
    lost because of system failture.
    Eventual duplicated tweets are removed during the data processing step.
    """

    def get_lost_tweets(last_tweets, db_ids):
        """
        Returns a list of tweets that are not in the database
        params:
            db_ids (list): Ids from the database
            last_tweets (list): Last tweets from the API
        returns:
            lost_tweets (list): Tweets that are not in the database
        """
        ## Get id from tweets from API
        tweets_ids = [tweet["id"] for tweet in last_tweets]
        difference = set(tweets_ids) - set(db_ids)
        ## Get the entire tweet content from the id
        lost_tweets = [tweet for tweet in last_tweets if tweet["id"] in difference]
        return lost_tweets

    def write_local_file(content):
        now = datetime.now()
        post_platform_id = content["id_str"]
        if (
            content["full_text"].startswith("RT @") == False
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

    ## Gets all twitter_user_ids from the database
    twitter_user_ids = postgres_tools.get_all_twitter_agent_platform_id()
    for twitter_user_id in set(twitter_user_ids):
        ## Initializes the Twitter_history_posts class
        twitter_obg = twitter_tools.Twitter_history_posts(user_id=twitter_user_id[0])
        ## Gets all tweets from the user from the database
        tweet_ids = postgres_tools.get_tweets_by_user_id(twitter_user_id[0])
        tweet_ids = [int(tweet[0]) for tweet in tweet_ids]
        ## Gets all tweets from the user from the API. Also sets delay to avoid stressing the API ##
        last_tweets = twitter_obg.get_last_tweets(delay_time=1)
        ## Sometimes the user is no longer available in the API.
        ## In this case, the script will skip the user
        if last_tweets != None:
            screen_name = last_tweets[0]["user"]["screen_name"]
            lost_tweets = get_lost_tweets(last_tweets, tweet_ids)
            print(
                f"{len(lost_tweets)} Tweets from {screen_name} that are in the API but not in the database"
            )
            time.sleep(5)
            for tweet_to_write in lost_tweets:
                ## A little delay to avoid running out of space in the local storage
                time.sleep(1.5)
                write_local_file(tweet_to_write)
    return


recover_lost_tweets()
