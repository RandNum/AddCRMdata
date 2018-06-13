import requests
import csv
import os
import json

from ratelimit import limits



user = "wiggins2000@gmail.com"
pw = "Deadalus82$$"
from requests.auth import HTTPBasicAuth

def authTest():
    print("Authenticating...")
    response = requests.get('https://api.relay42.com/v1/site-1151/', auth=HTTPBasicAuth('wiggins2000@gmail.com', 'Deadalus82$$'))
    print("response code: " + str(response.status_code))
#authTest()


def relayPOSTRequest():
    jsonData = [{
        "factName": "mostSearched",
        "factTtl": "65000000",
        "properties": {}
        }]

    url = 'https://api.relay42.com/v1/site-1151/profiles/2001/facts?partnerId=1151&forceInsert=false'

    headers = {"content-type":"application/json"}
    # call get service with headers and params
    response = requests.post(url, json=jsonData, headers=headers, auth=HTTPBasicAuth(user, pw))

    print("code:" + str(response.status_code))
    print("headers:" + str(response.headers))
    print("content:" + str(response.text))

relayPOSTRequest()


def parseCSV():
    print("parsing CSV file")
    filepath = os.path.join('/Users/laptop/Desktop/Relay42/FactDataForTCAssignment/', 'generatedDataFile_small_5.csv')
    with open(filepath, newline='') as csvfile:
        csvData = csv.reader(csvfile, delimiter=';')
        for row in csvData:
            print(', '.join(row))

#parseCSV()
