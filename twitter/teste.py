import tweepy
from tweepy import API
from tweepy import OAuthHandler
import twitter_credentials

auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
api = API(auth)
screen_name = 'jodmoreira'
def get_user_id(screen_name):
    id = api.get_user(screen_name).id
    return id

print(get_user_id(screen_name))
