import random
import time
from timeit import default_timer as timer
from datetime import datetime
import json
import asyncio
import aiohttp
import pytest
import asyncQueueRunner.asyncHttpQueueRunner as AQR
import pdb
import logging
import concurrent.futures
from enum import Enum, auto
#from asyncQueueRunner.asyncQueueRunner import AsyncHttpGet, AsyncHttpGetHandler, QueueRunner, ActionStatus

# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console = logging.StreamHandler()
# logger.addHandler(console)


def test_httpGetESI_MarketHistory(caplog):
    caplog.set_level(logging.INFO)
    startTime = datetime.utcnow()
    region_id = 10000002
    type_id = 34
    esiUrl = "https://esi.evetech.net/latest/"
    marketHistoryUrl = f"markets/{region_id}/history/"
    #filename = f"MarketHistory_{region_id}_{type_id}"+"_{datetime}"

    params = {'type_id': type_id}
    # responseHandler = AQR.AsyncHttpGetResponseHandler(storeResults=True)
    actions = []
    url = f"{esiUrl}{marketHistoryUrl}"
    action = AQR.AsyncHttpGet(url, params=params, storeResults=True)
    actions.append(action)
    queueRunner = AQR.AsyncHttpQueueRunner()
    queueRunner.execute(actions, 1)
    endTime = datetime.utcnow()
    for action in actions:
        printActionResult(action)
        assert action.completedActionStatus != None
    print(f"Total time for test: {endTime-startTime}")


def test_httpGetESI_MarketHistory_SaveToFile(caplog):
    caplog.set_level(logging.INFO)
    startTime = datetime.utcnow()
    region_id = 10000002
    type_id = 34
    esiUrl = "https://esi.evetech.net/latest/"
    marketHistoryUrl = f"markets/{region_id}/history/"
    path = "/Users/croaker/tmp"
    filename = f"MarketHistory_{region_id}_{type_id}"+f"_{startTime.strftime('%Y-%m-%dT%H.%M.%S')}.json"

    params = {'type_id': type_id}
    # responseHandler = AQR.AsyncHttpGetResponseHandler(storeResults=True)
    actions = []
    url = f"{esiUrl}{marketHistoryUrl}"
    action = AQR.AsyncHttpGet(
        url, params=params, storeResults=True, filename=filename, path=path, callback=AQR.saveFileCallback)
    actions.append(action)
    queueRunner = AQR.AsyncHttpQueueRunner()
    queueRunner.execute(actions, 1)
    endTime = datetime.utcnow()
    for action in actions:
        printActionResult(action)
        assert action.completedActionStatus != None
    print(f"Total time for test: {endTime-startTime}")

# @pytest.mark.asyncio


def test_httpGetESI(capsys):
    # responseHandler = AQR.AsyncHttpGetResponseHandler(storeResults=True)
    actions = []
    url = "https://esi.evetech.net/latest/markets/prices/?datasource=tranquility"
    action = AQR.AsyncHttpGet(url, storeResults=True)
    actions.append(action)
    queueRunner = AQR.AsyncHttpQueueRunner()
    queueRunner.execute(actions, 1)
    for action in actions:
        printActionResult(action)
        assert action.completedActionStatus != None


def test_httpGetESIx2(caplog, capsys):
    caplog.set_level(logging.INFO)
    startTime = datetime.utcnow()
    # responseHandler = AQR.AsyncHttpGetResponseHandler(storeResults=True)
    actions = []
    url = "https://esi.evetech.net/latest/markets/prices/?datasource=tranquility"
    action1 = AQR.AsyncHttpGet(url, storeResults=True)
    action2 = AQR.AsyncHttpGet(url, storeResults=True)
    actions.append(action1)
    actions.append(action2)
    queueRunner = AQR.AsyncHttpQueueRunner()
    queueRunner.execute(actions, 2)
    endTime = datetime.utcnow()
    for action in actions:
        printActionResult(action)
        assert action.completedActionStatus != None
    print(f"Total time for test: {endTime-startTime}")


def printActionResult(action):
    print("\n---- result----\n")
    print(f"Action: {action}")
    print(f"Response URL: {action.responseUrl}")
    print(f"Status Code: {action.completedActionStatus}")
    print(f"Status Reason: {action.completedActionStatusMessage}")
    print(f"EndTime:   {action.endTime}")
    print(f"StartTime: {action.startTime}")
    print(f"Formatted Start Time: {action.formatDateTime(action.startTime)}")
    if 'filename' in action.actionKwargs:
        print(f"Filename: {action.actionKwargs['filename']}")
    if 'path' in action.actionKwargs:
        print(f"Path: {action.actionKwargs['path']}")
    print(f"Time to complete action: {action.elapsedTime()}")
    if action.completedActionData != None:
        first100 = action.completedActionData[0:100]
    else:
        first100 = None
    print(f"Text Recieved(First 100 chars):\n{first100}")
    #print("test of a format string {foo}".format(foo="foo",end="end"))


def test_initAsyncHttpGet():
    action = AQR.AsyncHttpGet("Test Url")
    assert action.url == "Test Url"
    assert action.retryCounter == 0
    assert action.retryLimit == 5
    assert action.uuid != None
    # assert action.responseHandler == None

    # handler = AQR.AsyncHttpGetResponseHandler(storeResults=True)
    action2 = AQR.AsyncHttpGet("Test Url2", storeResults=True, retryLimit=6)
    assert action2.url == "Test Url2"
    assert action2.retryLimit == 6
    assert action.uuid != None
    # assert action2.responseHandler == handler


class ActionStatus(Enum):
    SUCCESS = 1
    RETRY = 2
    FAIL_NO_RETRY = 3
    ADD_ACTIONS = 4
