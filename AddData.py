import requests
import csv
import os
from requests.auth import HTTPBasicAuth
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo

user = "wiggins2000@gmail.com"
pw = "Deadalus82$$"

FIVE_MINUTES = 300

@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=50, period=FIVE_MINUTES)
def AddFactToProfile():
    print("parsing CSV file")
    filepath = os.path.join('/Users/laptop/Desktop/Relay42/FactDataForTCAssignment/', 'generatedDataFile_small_5.csv')
    filepathBig = os.path.join('/Users/laptop/Desktop/Relay42/FactDataForTCAssignment/', 'generatedDataFile_225000.csv')

    with open(filepathBig, newline='') as csvfile:
        csvData = csv.reader(csvfile, delimiter=';')
        headers = next(csvData)
        for row in csvData:
            pid = row[0]
            dest = row[1]
            org = row[2]
            JSONData = [{
                "factName": "mostSearched",
                "factTtl": "65000000",
                "properties":{"PartnerID":pid,
                              "Dest":dest,
                              "Orig":org}
                    }]

            url2 = "https://api.relay42.com/v1/site-1151/profiles/2001/facts?partnerId=" + pid + "&forceInsert=false"

            headers = {"content-type":"application/json"}
            # call get service with headers and params
            print("Adding Fact to PID " + pid)
            response = requests.post(url2, json=JSONData, headers=headers, auth=HTTPBasicAuth(user, pw))
            if response.status_code != 200:
                raise Exception('API response: {}'.format(response.status_code))
            print("code: " + str(response.status_code))
            #print("headers: " + str(response.headers))
            #print("content: " + str(response.text))

AddFactToProfile()