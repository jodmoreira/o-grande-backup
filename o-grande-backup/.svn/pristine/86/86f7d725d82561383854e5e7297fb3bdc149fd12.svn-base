import twitter_credentials
import tweepy

class OgbListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status._json)

auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)
obj_listener = OgbListener()
ogb_stream = tweepy.Stream(auth = api.auth, listener=obj_listener)
ogb_stream.filter(follow=["24761859"])