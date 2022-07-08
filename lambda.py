import json
import os
import requests
from requests.structures import CaseInsensitiveDict
import boto3
import datetime

# Env var: token from twitter dev api
# Trigger: setup to run daily at 0:05 and record the tweets of the day before
# Tweets are collected with the requests library and stored in S3 as json files (each file corresponds to a single day)

def _format_date(date):
    return datetime.datetime.strftime(date, "%Y-%m-%d")

def lambda_handler(event, context):
    
    # initial url 
    url = "https://api.twitter.com/2/tweets/search/recent?query=%23claustrovirtual+-is%3Aretweet+-is%3Aquote&tweet.fields=attachments%2Cauthor_id%2Ccontext_annotations%2Cconversation_id%2Centities%2Ccreated_at%2Cgeo%2Cin_reply_to_user_id%2Cid%2Clang%2Cpossibly_sensitive%2Cpublic_metrics%2Creferenced_tweets%2Creply_settings%2Csource%2Cwithheld%2Ctext&media.fields=alt_text%2Cduration_ms%2Cheight%2Cmedia_key%2Cpublic_metrics%2Cwidth%2Ctype%2Curl%2Cpreview_image_url&place.fields=contained_within%2Ccountry%2Cfull_name%2Cid%2Ccountry_code%2Cgeo%2Cname%2Cplace_type&poll.fields=voting_status%2Coptions%2Cid%2Cend_datetime%2Cduration_minutes&expansions=attachments.media_keys%2Cattachments.poll_ids%2Centities.mentions.username%2Cgeo.place_id%2Cin_reply_to_user_id%2Creferenced_tweets.id.author_id%2Creferenced_tweets.id%2Cauthor_id&max_results=100"

    # add the restriction of time dynamically (gather the tweets from yesterday only)
    today = datetime.datetime.now()
    start_day = today + datetime.timedelta(days=-6)
    end_day = start_day + datetime.timedelta(days=1)
    start_day = _format_date(start_day)
    end_day = _format_date(end_day)
    url += "&start_time={}T00:00:00Z&end_time={}T00:00:00Z".format(start_day, end_day)

    # authentication for get request
    token = os.environ['TOKEN']
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = "Bearer {}".format(token)
    
    resp = requests.get(url, headers=headers)
    
    assert resp.status_code == 200
    
    # store response in a file
    resp_json = json.dumps(resp.json(), indent = 4)
    fname = '/tmp/result.json'
    with open(fname, 'w') as fh:
        fh.write(resp_json)

    # upload file to s3
    bucket = 'tweets-claustro-virtual'
    s3 = boto3.client('s3')
    s3.upload_file(fname, bucket, '{}.json'.format(start_day))