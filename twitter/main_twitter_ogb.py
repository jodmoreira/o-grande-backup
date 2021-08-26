# YouTube Video: https://www.youtube.com/watch?v=rhBZqEWsZU4
import tweepy
from tweepy import API
from tweepy import OAuthHandler
import twitter_credentials
import time
import os
import traceback
import json

auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
api = API(auth)

#######################################
# Legado. Quando o sistema salvava os exports dentro do próprio diretório do script
# dir_script = os.path.realpath(__file__)
# dir = os.path.dirname(dir_script)
# usuarios = os.listdir(os.path.join(dir,'exports'))
#######################################

# try:
#     DIRETORIO_ARQUIVOS = 'C:\\Users\\Jod\\Google Drive\\O Grande Backup\\Twitter\\Documentos Brutos\\'
#     usuarios = os.listdir(DIRETORIO_ARQUIVOS)
#     usuarios = set(usuarios)
# except FileNotFoundError:
#     traceback.print_exc()
#     DIRETORIO_ARQUIVOS = '/usr/local/airflow/dirwindows/Google Drive/O Grande Backup/Twitter/Documentos Brutos/'
#     usuarios = os.listdir(DIRETORIO_ARQUIVOS)
#     usuarios = set(usuarios)

# try:
#     usuarios.remove('.tmp.drivedownload') #Retirando o documento que o google drive gera na pasta
# except KeyError:
#     pass
# usuarios.remove('desktop.ini') #Retirando o documento que o google drive gera na pasta
# usuarios.remove('Arquivo')
# usuarios.remove('legado')




def escritor(tweet, usuario):
    # dir_export = os.path.join(dir, fr'{DIRETORIO_ARQUIVOS}\{usuario}')
    try:
        diretorio_escrita = fr'{DIRETORIO_ARQUIVOS}{usuario}'
        doc_saida = open(diretorio_escrita,'a+', encoding='utf8')
    except FileNotFoundError:
        diretorio_escrita = f'{DIRETORIO_ARQUIVOS}{usuario}'
        doc_saida = open(diretorio_escrita,'a+', encoding='utf8')
    tweet = json.dumps(tweet._json, ensure_ascii=False)
    doc_saida.write(tweet)
    doc_saida.write('\n')
    doc_saida.close()

def pega_maior_id(usuario):
    '''Pega o maior id a partir dos exports para que o sistema
    continue a partir dele'''
    print(f'pegando o histórico do {usuario}')
    # arquivo_entrada = os.path.join(dir, fr'{DIRETORIO_ARQUIVOS}\{usuario}')
    try:
        doc = open(fr'{DIRETORIO_ARQUIVOS}\{usuario}', 'r', encoding='utf8')
    except FileNotFoundError:
        doc = open(fr'{DIRETORIO_ARQUIVOS}/{usuario}', 'r', encoding='utf8')
    doc = doc.readlines()
    doc_json = [json.loads(i) for i in doc]
    ids = [i['id'] for i in doc_json]
    maior_id = max(ids)
    maior_id = maior_id+1
    return maior_id
    
def executa_since_id(usuario):
    maior_id = pega_maior_id(usuario)
    tweets = api.user_timeline(id=usuario, tweet_mode='extended', count=200, since_id=maior_id)
    if len(tweets) < 1:
        print(f'nada encontrado de {usuario}' )
        return
    for tweet in tweets:
        id_tweet = tweet.id
        print(f'{id_tweet} de {usuario}')
        escritor(tweet, usuario)
    try:
        executa_since_id(usuario)
        print('indo para novo item')
    except Exception as e:
        print(e)
        print('sleep')
        time.sleep(15*60)
        executa_since_id(usuario)



##### Para primeira execução #####
def looper(ultimo_id, novo_usuario):
    tweets = api.user_timeline(id=novo_usuario, tweet_mode='extended', count=200, max_id=ultimo_id)
    if len(tweets) < 1:
        return
    for tweet in tweets:
        id_tweet = tweet.id
        print(f'{id_tweet} de {novo_usuario}')
        escritor(tweet, novo_usuario)
        ultimo_id = id_tweet-1
    try:
        looper(ultimo_id, novo_usuario)
        print('indo para novo item')
    except Exception as e:
        print(e)
        print('sleep')
        time.sleep(15*60)
        looper(ultimo_id, novo_usuario)

def primeira_execucao(novo_usuario):
    '''Executa a primeira análise e passa para o looper o valor 
    do ultimo id coletado para que ele dê continuidade'''
    for tweet in api.user_timeline(id=novo_usuario, tweet_mode='extended', count=200):
        id_tweet = tweet.id
        print(f'{id_tweet} de {novo_usuario}')
        escritor(tweet, novo_usuario)
        ultimo_id = id_tweet-1
    looper(ultimo_id, novo_usuario)

def get_user_id(screen_name):
    id = api.get_user(screen_name).id
    return id

    

if __name__ == '__main__':
    for usuario in usuarios:
        usuario = usuario
        try:
            executa_since_id(usuario)
        except tweepy.error.TweepError as e:
            print(e)
            if e == "Not authorized.":
                pass
