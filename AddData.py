__author__ = "Bryan C. Wiggins"
__maintainer__ = "Bryan Wiggins"
__email__ = "wiggins2000@gmail.com"


import requests
import csv
import os
from requests.auth import HTTPBasicAuth
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from threading import Timer
import UserPass

myuser = UserPass.myusername
mypw = UserPass.mypassword

FIVE_MINUTES = 300


class AddFactToProfile():

    def __init__(self):
        self.classVar = 0
        self.rt = RepeatedTimer(10, self.getProgress)

    @on_exception(expo, RateLimitException, max_tries=8)
    @limits(calls=100, period=FIVE_MINUTES)
    def addData(self):
        # filepath = os.path.join('testData', 'generatedDataFile_small_5.csv')
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
                    "properties": {"PartnerID": pid,
                                   "Dest": dest,
                                   "Orig": org}
                }]

                url2 = "https://api.relay42.com/v1/site-1151/profiles/2001/facts?partnerId=" + pid + "&forceInsert=false"

                headers = {"content-type": "application/json"}
                # call get service with headers and params
                response = requests.post(url2, json=JSONData, headers=headers, auth=HTTPBasicAuth(myuser, mypw))
                if response.status_code != 200:
                    raise Exception('API response: {}'.format(response.status_code))
                # print("code: " + str(response.status_code))
                else:
                    self.classVar = self.classVar + 1
                # print("headers: " + str(response.headers))
                # print("content: " + str(response.text))

    def getProgress(self):
        print("At row " + str(self.classVar) + "...")


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


print("starting upload of data...")
a = AddFactToProfile()  #autostarts
try:
    #Long running process of adding fact data
    a.addData()
finally:
    a.rt.stop()