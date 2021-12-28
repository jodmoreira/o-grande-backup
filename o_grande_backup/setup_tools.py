import aws_tools.s3_tools as s3_tools
import db_tools.postgres_tools as postgres_tools
import wf_tools.wf_tools as wf_tools
import twitter_tools.main_twitter_ogb as twitter_tools
import db_tools.postgres_tools as postgres_tools
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
