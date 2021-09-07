import twitter_credentials
import tweepy
import json
import s3
import database
from datetime import datetime, timedelta
import time
from urllib3.exceptions import ProtocolError


class OgbListener(tweepy.StreamListener):
    def on_status(self, status):
        content = status._json
        if content['text'].startswith("RT @") == False\
        and content['in_reply_to_user_id'] == None:
            #Just to adjust to my current timezone
            print(f'''new tweet from {content["user"]["screen_name"]} at {str(datetime.now()+ timedelta(hours=3))}''')
            s3.uploader(content)
            database_payload = (
                content["user"]["screen_name"],
                content["user"]["id_str"],
                content['id_str'],
                content['created_at'],
                str(datetime.now()),
                int(time.time())
                )
            database.log_new_tweet(database_payload)
    def on_error(self, status_code):
        if status_code == 420:
            time.sleep(10)
            print(f'Timeout at {str(datetime.now())}')
            return True

def main(ogb_stream, users_to_follow):
    try:
        ogb_stream.filter(follow=set(users_to_follow))
    except (ProtocolError, AttributeError) as e:
        print(e)
        print('Sleep')
        time.sleep(5)
        main(ogb_stream, users_to_follow)

if __name__ == '__main__':
    auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY,
                        twitter_credentials.CONSUMER_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN,
                        twitter_credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    obj_listener = OgbListener()
    ogb_stream = tweepy.Stream(auth = api.auth, listener=obj_listener)
    profiles = database.read_profiles()
    users_to_follow = [i[1] for i in profiles]
    main(ogb_stream, users_to_follow)