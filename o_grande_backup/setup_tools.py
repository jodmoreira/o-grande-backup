## Script to run things needed to setup the system
import aws_tools.s3_tools as s3_tools
import db_tools.postgres_tools as postgres_tools
import wf_tools.wf_tools as wf_tools
import twitter_tools.main_twitter_ogb as twitter_tools
import google_tools.drive_tools as drive_tools
import json
import pytz
from datetime import datetime


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
def get_lost_tweets_upload_s3():
    ## Gets all twitter_user_ids from the database
    twitter_user_ids = postgres_tools.get_all_twitter_users_id()
    for twitter_user_id in twitter_user_ids:
        ## Initializes the Twitter_history_posts class
        twitter_obg = twitter_tools.Twitter_history_posts(twitter_user_id[0])
        ## Gets all tweets from the user from the database
        tweet_ids = postgres_tools.get_tweets_by_user_id(twitter_user_id)
        last_tweets = twitter_obg.get_last_tweets()
        lost_tweets = twitter_obg.get_lost_tweets(last_tweets, tweet_ids)
        for tweet in lost_tweets:
            post_lake_dir = twitter_obg.lake_path_creator(tweet)
            ## Upload tweet to S3
            s3_tools.upload_file("ogb-lake", tweet, post_lake_dir)
            ## Add tweet to Postgres
            twitter_obg.add_new_tweet_to_db(tweet, post_lake_dir)


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
    update_webflow()
