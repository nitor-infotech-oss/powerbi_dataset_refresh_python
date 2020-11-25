import sys, os
import json
import sys, os
#import pandas as pd
import datetime
import time
from datetime import date
import calendar
import adal
import requests
from settings import *

def build_summary(refreshDetails):
    Summary = 'BI Report Refresh Summary:'
    for obj in refreshDetails:
        Summary = Summary + "\n {} - {} ".format(obj["name"], obj["status"] )
        if (obj['time'] != None):
            Summary = Summary + " - " + obj['time'][:-5]
    Summary = Summary + '\n\n'
    return Summary

def write_to_file(file_path, text):
    try:
        with open(file_path, "a") as myfile:
            myfile.write(text)
    except:
        print("Failed to write to logs file.")


def read_json_from_file(json_file_path):
    datasets = []
    try:
        with open(json_file_path) as f:
          datasets = json.load(f)
    except:
        print("+++"*20)
        print("+++"*20)
        print("Failed to read from json config file. Please check the configurations.")
        print("+++"*20)
        print("+++"*20)
        raise
    return datasets

try:
    my_date = date.today()
    dt = datetime.datetime.today()

    datasets = read_json_from_file(JSON_FILE_PATH)

    DatasetFrequencyMapping = {}
    for i in datasets:
        DatasetFrequencyMapping[i["DataSetName"]] = [i["RefreshFrequencyType"], i["RefreshFrequency"]]


    # Get Auth Token
    context       = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    token         = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)
    access_token  = token.get('accessToken')

    # Set Header for Requests
    header        = {'Authorization': f'Bearer {access_token}'}

    # Get All Datasets
    responseDetails = []
    for group_id in groupids:
        refresh_url     = 'https://api.powerbi.com/v1.0/myorg/groups/{}/datasets'.format(group_id)
        r               = requests.get(url=refresh_url, headers=header)
        responseDetails = responseDetails + json.loads(r.content)['value']

    refreshDetails = []
    for dat in responseDetails:
        if dat['name'] in DatasetFrequencyMapping.keys():
            frequency = DatasetFrequencyMapping[dat['name']]
            if (frequency[0] == "Daily" or (frequency[0] == "Weekly" and frequency[1]==calendar.day_name[my_date.weekday()]) or (frequency[1] == "Monthly" and frequency[1] == dt.day)):
                print("Now Refreshing - ", dat['name'], dat['id'],  frequency)
                print("+++++"*20)
                dataset_id = dat['id']
                # # Refresh the Dataset
                refresh_url        = 'https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/refreshes?$top=1'.format(group_id, dataset_id)
                r                  = requests.post(url=refresh_url, headers=header)
                checkRefreshStatus = True
                while checkRefreshStatus:
                    try:
                        refresh_history_url = 'https://api.powerbi.com/v1.0/myorg/datasets/{}/refreshes?$top=1'.format(dataset_id)
                        historyResponse     = requests.get(url=refresh_history_url, headers=header)
                        responseObj         = json.loads(historyResponse.content)['value'][0]
                        refreshStatus       = json.loads(historyResponse.content)['value'][0]['status']
                        endTime = None
                        if 'endTime' in responseObj.keys():
                            endTime = responseObj['endTime']
                    except Exception as e:
                        refreshStatus       =  "Failed"
                    if (refreshStatus == "Completed"):
                        refObj = {'name': dat['name'], 'status': "Completed", 'time': endTime}
                        refreshDetails.append(refObj)
                        checkRefreshStatus = False
                    elif(refreshStatus == "Failed"):
                        refObj = {'name': dat['name'], 'status': "Failed", 'time': None}
                        refreshDetails.append(refObj)
                        checkRefreshStatus = False
                    time.sleep(30)

    if(len(refreshDetails) > 0):
        summary = build_summary(refreshDetails)
        write_to_file(LOG_FILE_PATH, summary)
except Exception as e:
    raise
