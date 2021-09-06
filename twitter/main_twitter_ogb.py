# YouTube Video: https://www.youtube.com/watch?v=rhBZqEWsZU4
import tweepy
from tweepy import API
from tweepy import OAuthHandler
import twitter_credentials
import time
import json
import s3
import datetime

auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
api = API(auth)


##### For the first execution #####
def looper(ultimo_id, new_user):
    tweets = api.user_timeline(id=new_user, tweet_mode='extended', count=200, max_id=ultimo_id)
    if len(tweets) < 1:
        return
    for tweet in tweets:
        id_tweet = tweet.id
        print(f'{id_tweet} from {new_user}')
        s3.upload(tweet._json)
        ultimo_id = id_tweet-1
    try:
        print('Going to new item')
        looper(ultimo_id, new_user)
    except Exception as e:
        print(e)
        print('sleep')
        time.sleep(15*60)
        looper(ultimo_id, new_user)

def first_execution(new_user):
    '''
    Runs the frist API loop and returns the first 200 tweets and
    then starts looping
    '''
    for tweet in api.user_timeline(id=new_user, tweet_mode='extended', count=200):
        id_tweet = tweet.id
        print(f'{id_tweet} from {new_user}')
        s3.upload(tweet._json)
        last_id = id_tweet-1
    looper(last_id, new_user)

def get_user_id(screen_name):
    id = api.get_user(screen_name).id
    return id
##
##
## Legacy functions
def writer(tweet, user):
    try:
        diretorio_escrita = fr'{FILES_DIR}{user}'
        doc_saida = open(diretorio_escrita,'a+', encoding='utf8')
    except FileNotFoundError:
        diretorio_escrita = f'{FILES_DIR}{user}'
        doc_saida = open(diretorio_escrita,'a+', encoding='utf8')
    tweet = json.dumps(tweet._json, ensure_ascii=False)
    doc_saida.write(tweet)
    doc_saida.write('\n')
    doc_saida.close()

def get_greater_id(user):
    try:
        doc = open(fr'{DIRETORIO_ARQUIVOS}\{user}', 'r', encoding='utf8')
    except FileNotFoundError:
        doc = open(fr'{DIRETORIO_ARQUIVOS}/{user}', 'r', encoding='utf8')
    doc = doc.readlines()
    doc_json = [json.loads(i) for i in doc]
    ids = [i['id'] for i in doc_json]
    maior_id = max(ids)
    maior_id = maior_id+1
    return maior_id

def run_since_id(user):
    maior_id = get_greater_id(user)
    tweets = api.user_timeline(id=user, tweet_mode='extended', count=200, since_id=maior_id)
    if len(tweets) < 1:
        print(f'nothing found from {user}' )
        return
    for tweet in tweets:
        id_tweet = tweet.id
        print(f'{id_tweet} from {user}')
        writer(tweet, user)
    try:
        run_since_id(user)
        print('New item')
    except Exception as e:
        print(e)
        print('sleep')
        time.sleep(15*60)
        run_since_id(user)


if __name__ == '__main__':
    first_execution('jodmoreira')