# -*- coding: utf-8 -*-
__author__ = "Akshay Nar"

# python dependencies
import requests
import json
import ConfigParser


def configure_api_parameters():
    config = ConfigParser.ConfigParser()

    #to be set/changed while running executing
    config.read('E:/Collinson_Task/apiparameters.ini')
    stage = 'FETCH_DATA'
    url = config.get(stage,'API_URL')
    x_api_key = config.get(stage,'X-API-KEY')
    x_api_id = config.get(stage,'X-API-ID')

    return (url,x_api_key,x_api_id)

def make_api_call():
    url,x_api_key,x_api_id = configure_api_parameters()
    headers = {'Content-Type': 'application/json', 'x-api-id' : x_api_id,'x-api-key' : x_api_key}
    payload = {
        'query': 'select * from tweet_tags',
        'type' : 'select',
        'records' : 'one'}

    response = requests.post(url, data=json.dumps(payload), headers=headers)

    json_data = json.loads(response.text)
    print(json_data)

if __name__ == '__main__':
    make_api_call()

