import boto3
import json
from datetime import date, datetime
import time

s3 = boto3.client('s3')

def uploader(content):
    '''
    Uploads json to AWS S3
    '''
    today = date.today()
    year = today.year
    month = today.month
    day = today.day
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    screen_name = str(content["user"]["screen_name"])
    screen_name = screen_name.replace(' ','')
    try:
        s3.put_object(Body=(bytes(json.dumps(content).encode('UTF-8'))),
        Bucket='ogb-lake',
        Key=f'social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{screen_name}/{content["id_str"]}__{now}.json')
    except botocore.exceptions.NoCredentialsError as e:
        print(e)
        time.sleep(1)
        uploader(content)

def downloader(key, prefix=None):
    '''
    Downloads all files inside a bucket. Prefix can be sent optionally
    '''
    all_objects = s3.list_objects_v2(Bucket = 'ogb-lake', Prefix=prefix, MaxKeys=1000)
    keys = [i.get('Key') for i in all_objects.get('Contents')]
    while all_objects.get('NextContinuationToken') != None:
        all_objects = s3.list_objects_v2(Bucket = 'ogb-lake', 
        MaxKeys=1000,
        ContinuationToken=all_objects['NextContinuationToken']
        )
        more_keys = [i.get('Key') for i in all_objects.get('Contents')]
        keys = keys + more_keys
    for i, file_name in enumerate(set(keys)):
        response = s3.download_file('ogb-lake', file_name, file_name)
        print(f'Downloading {file_name}')

if __name__ == '__main__':
    list_all_folders_from_landing_zone()