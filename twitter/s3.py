import boto3
import json
from datetime import date, datetime

s3 = boto3.client('s3')

def uploader(content):
    '''
    Uploads json to AWS S3
    '''
    today = date.today().strftime('%d-%m-%Y')
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    screen_name = str(content["user"]["screen_name"])
    screen_name = screen_name.replace(' ','')
    try:
        s3.put_object(Body=(bytes(json.dumps(content).encode('UTF-8'))),
        Bucket='ogb-dados-gerais',
        Key=f'landing_zone/ingestion_date={today}/{screen_name}/{screen_name}__{content["id_str"]}__{now}.json')
    except botocore.exceptions.NoCredentialsError as e:
        print(e)
        time.sleep(1)
        uploader(content)

def downloader(key, prefix=None):
    '''
    Downloads all files inside a bucket. Prefix can be sent optionally
    '''
    all_objects = s3.list_objects_v2(Bucket = 'ogb-dados-gerais', Prefix=prefix, MaxKeys=1000)
    keys = [i.get('Key') for i in all_objects.get('Contents')]
    while all_objects.get('NextContinuationToken') != None:
        all_objects = s3.list_objects_v2(Bucket = 'ogb-dados-gerais', 
        MaxKeys=1000,
        ContinuationToken=all_objects['NextContinuationToken']
        )
        more_keys = [i.get('Key') for i in all_objects.get('Contents')]
        keys = keys + more_keys
    for i, file_name in enumerate(set(keys)):
        response = s3.download_file('ogb-dados-gerais', file_name, file_name)
        print(f'Downloading {file_name}')

if __name__ == '__main__':
    list_all_folders_from_landing_zone()