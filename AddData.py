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





def parseCSV():
    print("parsing CSV file")
    filepath = os.path.join('/Users/laptop/Desktop/Relay42/FactDataForTCAssignment/', 'generatedDataFile_small_5.csv')
    with open(filepath, newline='') as csvfile:
        csvData = csv.reader(csvfile, delimiter=';')
        for row in csvData:
            pid = row[0]
            dest = row[1]
            org = row[2]

    thisJSONObj = convertToAPIFormat(pid, dest, org)
    return thisJSONObj

#parseCSV()


def convertToAPIFormat(a, b, c):
    csvList = [{
        "factName": "mostSearched",
        "factTtl": "65000000",
        "properties":{"PartnerID":a,
                      "Dest":b,
                      "Orig":c}
            }]


    return csvList


def relayPOSTRequest():
    jsonData = [{
        "factName": "mostSearched",
        "factTtl": "65000000",
        "properties": {"this":"that",
                       "this":"that"}
        }]


    newJSONData = parseCSV()
    print(newJSONData)

    url = 'https://api.relay42.com/v1/site-1151/profiles/2001/facts?partnerId=1151&forceInsert=false'

    headers = {"content-type":"application/json"}
    # call get service with headers and params
    response = requests.post(url, json=newJSONData, headers=headers, auth=HTTPBasicAuth(user, pw))

    print("code: " + str(response.status_code))
    print("headers: " + str(response.headers))
    print("content: " + str(response.text))

relayPOSTRequest()