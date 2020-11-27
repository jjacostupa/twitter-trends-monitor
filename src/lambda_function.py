import os
import tweepy
import json
import boto3
#import pandas as pd

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

bucket_name = os.environ['BUCKET_NAME']

s3_client = boto3.client('s3')

#Twitter API auth
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(
    auth, wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True,
    retry_count=30,
    retry_delay=30
)

def lambda_handler(event, context):
    
    trends_available = api.trends_available()
    
    trends_local_path = '/tmp/avreg'
    trends_filename = 'available_regions.json'
    trends_s3_key = f'regions/{trends_filename}'
    
    data_local_path = '/tmp/data'
    
    if not os.path.isdir(trends_local_path):
        os.makedirs(trends_local_path)
        
    if not os.path.isdir(data_local_path):
        os.makedirs(data_local_path)
        
    with open(f'{trends_local_path}/{trends_filename}', 'w') as outfile:
        json.dump(trends_available, outfile)
        
    s3_client.upload_file(
        f'{trends_local_path}/{trends_filename}',
        bucket_name,
        f'regions/{trends_filename}'
    )
    os.remove(f'{trends_local_path}/{trends_filename}')
    
    print(f'Available regions: {len(trends_available)}')
    
    final_trends = []
    
    for region in trends_available:
        local_woeid = region['woeid']
        local_name = region['name']
        local_placetype = region['placeType']['name']
        local_parentid = region['parentid']
        local_country = region['country']
        local_countrycode = region['countryCode']
        
        if local_placetype in ['Country','Supername']:
        
            print(f'Getting local trends from {local_name}...')
            
            local_trends = api.trends_place(local_woeid)
            
            timestamp = local_trends[0]['as_of']
            
            
            actual_trends = local_trends[0]['trends']
            
            for i in range(len(actual_trends)):
                actual_trends[i]['woeid'] = local_woeid
                actual_trends[i]['name'] = local_name
                actual_trends[i]['countrycode'] = local_countrycode
                actual_trends[i]['as_of'] = timestamp
            
            final_trends += actual_trends
            
    local_filename = f'{data_local_path}/all-{timestamp}.json'
            
    # Save to local dir
    with open(local_filename, 'w') as outfile:
        json.dump(final_trends, outfile)
                
    # Upload to s3
    s3_client.upload_file(
        local_filename,
        bucket_name,
        f'data/all-{timestamp}.json'
    )
            
    os.remove(local_filename)
            