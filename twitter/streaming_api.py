import twitter_credentials
import tweepy
import json
import s3
import database



class OgbListener(tweepy.StreamListener):
    def on_status(self, status):
        content = status._json
        if content['text'].startswith("RT @") == False\
        and content['in_reply_to_user_id'] == None:
            print(f'new tweet from {content["user"]["screen_name"]}')
            s3.upload(content)

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
    ogb_stream.filter(follow=set(users_to_follow))