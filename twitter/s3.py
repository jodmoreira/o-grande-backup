import boto3
import json

s3 = boto3.client('s3')

def upload(content):
    screen_name = str(content["user"]["screen_name"])
    screen_name = screen_name.replace(' ','')
    s3.put_object(Body=(bytes(json.dumps(content).encode('UTF-8'))),
    Bucket='ogb-dados-gerais',
    Key=f'{screen_name}/{screen_name}_{content["id_str"]}.json',
    StorageClass='STANDARD_IA')
