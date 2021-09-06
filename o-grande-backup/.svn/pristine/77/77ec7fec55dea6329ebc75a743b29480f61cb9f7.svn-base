import boto3
from datetime import datetime


cli = boto3.client('s3')
cli.put_object(
  Body='The time now is '+str(datetime.now()),
  Bucket='ogb-dados-gerais',
  Key='ec2.txt')
