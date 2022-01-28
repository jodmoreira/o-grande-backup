## Script to run things needed to setup the system
import aws_tools.s3_tools as s3_tools
import db_tools.postgres_tools as postgres_tools
import wf_tools.wf_tools as wf_tools
import twitter_tools.main_twitter_ogb as twitter_tools
import google_tools.drive_tools as drive_tools
import json
import pytz
from datetime import datetime, timedelta
import os
import time


SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
NON_AGENT = os.environ.get("NON_AGENT")  # Used to fill db entry for non agents data
NON_AGENT_ID = os.environ.get(
    "NON_AGENT_ID"
)  # Used to fill db entry for non agents data


def add_agents_to_db():
    all_sites = eval(wf_tools.get_all_sites())
    for site in all_sites:
        if site["name"] == "ogb":
            site_id = site["_id"]
        else:
            pass
    all_collections = eval(wf_tools.get_all_collections(site_id))
    for collection in all_collections:
        if collection["name"] == "Agents":
            collection_id = collection["_id"]
        else:
            pass
    count = 0
    total = 1
    agents_output = list()
    while count < total:
        all_items = wf_tools.get_all_items_from_collections(
            site_id, collection_id, offset=count
        )
        all_items = json.loads(all_items)
        count = all_items["offset"] + 100
        total = all_items["total"]
        for i in all_items["items"]:
            try:
                agent_names = {
                    "name": i["name"],
                    "twitter_address": i["endereco-do-twitter"],
                }
            except:
                pass
            # agent_names = [ {'name':i for i in all_items["items"]]
            agents_output += agent_names
    return agents_output


def add_twitter_profile_to_db():
    all_sites = eval(wf_tools.get_all_sites())
    for site in all_sites:
        if site["name"] == "ogb":
            site_id = site["_id"]
        else:
            pass
    all_collections = eval(wf_tools.get_all_collections(site_id))
    for collection in all_collections:
        if collection["name"] == "Agents":
            collection_id = collection["_id"]
        else:
            pass
    count = 0
    total = 1
    twitter_output = list()
    while count < total:
        all_items = wf_tools.get_all_items_from_collections(
            site_id, collection_id, offset=count
        )
        all_items = json.loads(all_items)
        count = all_items["offset"] + 100
        total = all_items["total"]
        twitter_names = list()
        for i in all_items["items"]:
            try:
                dict_loop = {
                    "name": i["name"],
                    "twitter": i["endereco-do-twitter"].replace(
                        "https://twitter.com/", ""
                    ),
                }
                twitter_names.append(dict_loop)
            except:
                pass
        twitter_output += twitter_names
    return twitter_output


# setup_tools.get_lost_tweets_upload_s3()
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
        print(content)
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
            f"{SCRIPT_PATH}/twitter_tools/temp_storage/test/{screen_name}-{post_platform_id}__{now}.json",
        )

    ## Gets all twitter_user_ids from the database
    twitter_user_ids = postgres_tools.get_all_twitter_agent_platform_id()
    for twitter_user_id in twitter_user_ids:
        ## Initializes the Twitter_history_posts class
        twitter_obg = twitter_tools.Twitter_history_posts(user_id=twitter_user_id[0])
        ## Gets all tweets from the user from the database
        tweet_ids = postgres_tools.get_tweets_by_user_id(twitter_user_id[0])
        tweet_ids = [int(tweet[0]) for tweet in tweet_ids]
        ## Gets all tweets from the user from the API. Also sets delay to avoid stressing the API ##
        last_tweets = twitter_obg.get_last_tweets(delay_time=1)
        lost_tweets = get_lost_tweets(last_tweets, tweet_ids)
        print(len(tweet_ids), "all tweets from this users stored in the database")
        print(len(last_tweets), "Last tweets from the API")
        print(len(lost_tweets), "Tweets that are in the API but not in the database")
        for tweets_to_write in lost_tweets:
            ## A little delay to avoid running out of space in the local storage
            time.sleep(1)
            write_local_file(tweets_to_write)
    return


def update_webflow():
    def get_agents_from_db():
        agents = postgres_tools.get_all_agents()
        return agents

    def get_agents_from_drive():
        agents = drive_tools.list_files_in_dir()
        return agents

    def get_agents_from_webflow():
        agents = wf_tools.get_all_agents()
        return agents

    def get_agents_from_s3():
        bucket = "ogb-lake"
        prexif = "social_media/twitter/sharing_zone/social_media/twitter/"
        agents = s3_tools.list_files(bucket, prexif)
        output = []
        for agent in agents:
            dados = {}
            dados["url"] = agent
            dados["name"] = agent.split("/")[-2]
            output.append(dados)
        return output

    def update_webflow(agents):
        for agent in agents:
            site_id = wf_tools.get_all_sites()[0]["_id"]
            collection_id = wf_tools.get_all_collections(site_id)[0]["_id"]
            field_name = "backup-do-twitter"
            wf_tools.patch_collection_item(
                collection_id, agent["_id"], field_name, agent["url"]
            )

    def wf_drive_matching():
        all_agents_wf = get_agents_from_webflow()
        all_agents_drive = get_agents_from_drive()
        dados_agents = []
        for ad in all_agents_drive:
            for aw in all_agents_wf:
                try:
                    if (
                        aw["endereco-do-twitter"].split("/")[-1].lower()
                        == ad["name"].lower()
                    ):
                        dados_agents.append(
                            {
                                "_id": aw["_id"],
                                "endereco-do-twitter": aw["endereco-do-twitter"],
                                "url": ad["url"].replace("edit?usp=drivesdk", ""),
                            }
                        )
                except KeyError:
                    pass

    def wf_s3_matching():
        all_agents_wf = get_agents_from_webflow()
        all_agents_s3 = get_agents_from_s3()

        dados_agents = []
        for a3 in all_agents_s3:
            for aw in all_agents_wf:
                try:
                    if (
                        aw["endereco-do-twitter"].split("/")[-1].lower()
                        == a3["name"].lower()
                    ):
                        dados_agents.append(
                            {
                                "_id": aw["_id"],
                                "endereco-do-twitter": aw["endereco-do-twitter"],
                                "url": f'https://ogb-lake.s3.us-east-2.amazonaws.com/{a3["url"]}',
                            }
                        )
                except KeyError:
                    pass
        return dados_agents

    update_webflow(wf_s3_matching())


if __name__ == "__main__":
    recover_lost_tweets()
