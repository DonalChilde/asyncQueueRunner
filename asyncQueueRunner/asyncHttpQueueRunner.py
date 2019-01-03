"""

Things to do:
- Add optional access to actions to QueueRunner class, support access to
    new actions generated inside coro loop, ie. in action handler.

DONE: - add uuid support to action to make it easy to id

- add logging all over to figure out what is going on

- add documentaion all over so I don't loose the train of thought.

- Use type hints

- How to abort actions if unable to connect to internet.

- make a queuerunner class that holds the queue, and has convenience functions like add, retry
    retry, fail, failall, newAction

DONE: - move methods from testing over to production file.

- add tests for all methods, refactor as necessary.

- Add repr to classes

- keep a history of action response status and status message

DONE: - make a callback that can save files

- TODO refactor to use aiohttp.Clientsession.request
    - TODO refactor to allow different internal callbacks, to support using above.
    - TODO make a factory method to generate appropriate defaults 

- TODO change callbacks to support list of callbacks, to be run sequentially

- TODO support passing params for session

- TODO delay for loop closing re aiohttp client session docs




"""

from __future__ import annotations

from timeit import default_timer as timer
from datetime import datetime, timedelta
from pathlib import Path
#from time import perf_counter_ns as timer
import asyncio
import aiohttp
import logging
import uuid
import concurrent.futures
from asyncQueueRunner.asyncQueueRunner import AsyncAction, ActionStatus
from typing import List


MAX_CONSUMERS = 100
MAX_QUEUE_SIZE = 0
DATETIMESTRING = datetime.utcnow().strftime('%Y-%m-%dT%H.%M.%S')


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console = logging.StreamHandler()
# logger.addHandler(console)


class RequestState(object):
    """
    """

    def __init__(self, action: AsyncHttpRequest, response: aiohttp.client.ClientResponse, queue: asyncio.Queue, responseText: str = "", **kwargs):
        self.action = action
        self.response = response
        self.queue = queue
        self.responseText = responseText
        self.stateKwargs = kwargs


class AsyncHttpRequest(object):
    def __init__(self, method: str, url: str, requestParams: dict = None, storeResults: bool = False, retryOnFail: bool = True, retryLimit: bool = 5, callback=None, internalParams: dict = None):
        # TODO enable default response handler

        #super().__init__(actionHandler=actionHandler, retryLimit=retryLimit)
        self.method = method
        self.url = url
        if requestParams is None:
            self.requestParams = {}
        else:
            self.requestParams = requestParams
        self.uuid = uuid.uuid4()
        self.storeResults = storeResults
        self.retryOnFail = retryOnFail
        self.retryCounter = 0
        self.retryLimit = retryLimit
        self.callback = callback
        if internalParams is None:
            self.internalParams = {}
        else:
            self.internalParams = internalParams
        self.completedActionStatus = None
        self.completedActionStatusMessage = None
        self.completedActionData = None
        self.responseUrl = None
        self.startTime = 0
        self.endTime = 0
        # self.actionKwargs = kwargs

    def __repr__(self):
        return (f'<{self.__class__.__name__}('
                f'url={self.url!r}, requestParams={self.requestParams!r}, '
                f'storeResults={self.storeResults}, retryOnFail={self.retryOnFail}, retryLimit={self.retryLimit})>')

    async def doAsyncAction(self, queue: asyncio.Queue, session: aiohttp.ClientSession):
        self.retryCounter += 1
        self.startTime = datetime.utcnow()
        try:
            async with session.request(self.method, self.url, **self.requestParams) as response:
                state = RequestState(
                    action=self, response=response, queue=queue)
                state = await self.waitForResponseText(state)
                if self.retryOnFail:
                    state = await self.checkForRetry(state)
                if self.callback:
                    state = await self.callback(state)
                if self.storeResults:
                    self.completedActionData = state.responseText
                return
        except asyncio.TimeoutError as e:
            logger.exception(e)

        except aiohttp.ClientConnectionError as e:
            logger.exception(f"Connection error in {self} ")
            raise e

        except aiohttp.ClientError as e:
            logger.exception(e)
        finally:
            self.endTime = datetime.utcnow()

    async def checkForRetry(self, state: RequestState) -> RequestState:
        if state.response.status == 200:  # ActionStatus.SUCCESS
            return state
        if state.response.status == 404:  # ActionStatus.FAIL_NO_RETRY
            return state
        # 503 Service Unavailable, 504 Gateway Timeout
        if state.response.status in [503, 504]:
            # TODO either reset action.completed statuses, or implement list of statuses
            await state.queue.put(state.action)
            logger.info(
                f"Response status {state.response.status} recieved. Resubmitting {state.action} to queue.")
            return state
        logger.info(
            f"Action: {state.action} recieved unhandled response status of {state.response.status}: {state.response.reason}")
        # TODO remove this after finding the most common retry senarios
        raise NotImplementedError(f"Status Code: {state.response.status}")
        # return

    async def waitForResponseText(self, state: RequestState) -> RequestState:
        state.responseText = await state.response.text()
        state.action.completedActionStatus = state.response.status
        state.action.completedActionStatusMessage = state.response.reason
        state.action.responseUrl = state.response.url

        return state

    def elapsedTime(self) -> timedelta:
        # return f"{self.endTime-self.startTime:.3f}s"
        return self.endTime-self.startTime

    def formatDateTime(self, datetime: datetime.datetime) -> str:
        return datetime.strftime('%Y-%m-%dT%H.%M.%S')

    @classmethod
    def get(cls, url: str, requestParams: dict = None, storeResults: bool = False, retryOnFail: bool = True, retryLimit: int = 5, callback=None, internalParams: dict = None):
        return cls('get', url, requestParams=requestParams, storeResults=storeResults, retryOnFail=retryOnFail, retryLimit=retryLimit, callback=callback, internalParams=internalParams)


async def saveFileCallback(state: RequestState) -> RequestState:
    filename = ""
    path = ""
    if state.action.internalParams.get('saveToFile'):
        if state.action.internalParams.get('saveToFile').get('filename'):
            filename = state.action.internalParams.get(
                'saveToFile').get('filename')
        else:
            logger.error(
                f"No filename in action {state.action}. File save aborted.")
            return state

        if state.action.internalParams.get('saveToFile').get('path'):
            path = state.action.internalParams.get('saveToFile').get('path')
        else:
            logger.error(
                f"No path in action {state.action}. File save aborted.")
            return state
    else:
        logger.error(
            f"No save to file info in action {state.action}. File save aborted.")
        return state

    saveToFile(path, filename, state.responseText)
    return state


def saveToFile(path: str, filename: str, text: str, overwrite: bool = False):
    basePath = Path(path)
    if not basePath.is_dir():
        logger.error(
            f"{basePath} is not a directory, or it doesn't exist. File save aborted.")
        return
    fullPath = basePath / filename
    if not overwrite and fullPath.exists():
        logger.error(
            f"{fullPath} exists, will not overwrite. File save aborted")
        return

    try:
        with open(fullPath, 'wt') as file:
            file.write(text)
    except Exception:
        logger.exception(f"Error trying to save a file with path: {fullPath}")


class AsyncHttpQueueRunner(object):

    def __init__(self):
        pass

    def execute(self, actions: List[AsyncHttpRequest], connections: int, sessionParams: {} = None):
        if sessionParams is None:
            sessionParams = {}
        try:
            asyncio.run(self._doActions(
                actions, connections, sessionParams), debug=True)
        except concurrent.futures.CancelledError:
            logger.debug("execute: Another concurrent.futures.CancelledError")
        except Exception:
            logger.exception(
                "Unhandled exception made it all the way to 'execute'")

    async def _doActions(self, actions: List[AsyncHttpRequest], connections: int, sessionParams: {} = None):
        queue = asyncio.Queue()
        async with aiohttp.ClientSession(**sessionParams) as session:
            consumers = [asyncio.ensure_future(self._consumer(queue, session))
                         for _ in range(connections)]
            await self._fillQueue(queue, actions)
            # wait for all item's inside the main_queue to get task_done
            await queue.join()
        # cancel all coroutines
        for consumer_future in consumers:
            consumer_future.cancel()

    async def _consumer(self, queue: asyncio.Queue, session: aiohttp.ClientSession):
        while True:
            action = await queue.get()
            try:
                if action.retryLimit == -1 or action.retryCounter <= action.retryLimit:
                    await action.doAsyncAction(queue, session)
                    queue.task_done()
                else:
                    queue.task_done()
                    continue
            except aiohttp.ClientConnectorError as e:
                # Unable to connect to Host - No internet?
                # Signals task done to queue, tries to pass exception up the chain,
                # but it never leaves the loop?
                queue.task_done()
                raise e

            except Exception as e:
                logger.exception(e)

            logging.info(f"Time to complete action: {action.elapsedTime()}")

    async def _fillQueue(self, queue: asyncio.Queue, actions: List[AsyncHttpRequest]):
        # Add some performance tracking data to the queue
        queue.highCount = len(actions)
        queue.startTime = timer()
        queue.lastReport = timer()
        for action in actions:
            await queue.put(action)


# class ActionStatus(Enum):
#     SUCCESS = 1
#     RETRY = 2
#     FAIL_NO_RETRY = 3
#     ADD_ACTIONS = 4
