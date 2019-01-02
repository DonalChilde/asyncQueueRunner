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

- TODO make a callback that can save files

- TODO refactor to use aiohttp.Clientsession.request
    - TODO refactor to allow different internal callbacks, to support using above.
    - TODO make a factory method to generate appropriate defaults 

- TODO change callbacks to support list of callbacks, to be run sequentially




"""

from __future__ import annotations

from timeit import default_timer as timer
from datetime import datetime
from pathlib import Path
#from time import perf_counter_ns as timer
import asyncio
import aiohttp
import logging
import uuid
import concurrent.futures
from asyncQueueRunner.asyncQueueRunner import AsyncAction, ActionStatus


MAX_CONSUMERS = 100
MAX_QUEUE_SIZE = 0
DATETIMESTRING = datetime.utcnow().strftime('%Y-%m-%dT%H.%M.%S')


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console = logging.StreamHandler()
# logger.addHandler(console)


class ActionStateGet(object):
    """
    """

    def __init__(self, action: AsyncHttpGet, response: aiohttp.client.ClientResponse, queue: asyncio.Queue, responseText: str = "", **kwargs):
        self.action = action
        self.response = response
        self.queue = queue
        self.responseText = responseText
        self.stateKwargs = kwargs


class AsyncHttpGet(object):
    """Holds info for an Http Get action, and data from its Response.

    This class holds the info required for a Get action, and can store
    the info from a response. Typical use is to make a list of these actions
    by supplying a URL, giving that list to a QueueRunner.execute, and then 
    manipulating the returned data. 
    """

    def __init__(self, url: str, getDict: dict = None, storeResults: bool = False, retryOnFail: bool = True, retryLimit: bool = 5, callback=None, internalParams: dict = None):
        # TODO enable default response handler

        #super().__init__(actionHandler=actionHandler, retryLimit=retryLimit)
        self.url = url
        if getDict is None:
            self.getDict = {}
        else:
            self.getDict = getDict
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
                f'url={self.url!r}, getDict={self.getDict!r}, '
                f'storeResults={self.storeResults}, retryOnFail={self.retryOnFail}, retryLimit={self.retryLimit})>')

    async def doAsyncAction(self, queue, session):
        self.retryCounter += 1
        self.startTime = datetime.utcnow()
        try:
            async with session.get(self.url, **self.getDict) as response:
                state = ActionStateGet(
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

    async def checkForRetry(self, state: ActionStateGet) -> ActionStateGet:
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

    async def waitForResponseText(self, state: ActionStateGet) -> ActionStateGet:
        state.responseText = await state.response.text()
        state.action.completedActionStatus = state.response.status
        state.action.completedActionStatusMessage = state.response.reason
        state.action.responseUrl = state.response.url

        return state

    def elapsedTime(self):
        # return f"{self.endTime-self.startTime:.3f}s"
        return self.endTime-self.startTime

    def formatDateTime(self, datetime):
        return datetime.strftime('%Y-%m-%dT%H.%M.%S')


async def saveFileCallback(state: ActionStateGet) -> ActionStateGet:
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


class SaveResponseToFile(object):
    def __init__(self):
        super.__init__()

    async def manipulateResponseText(self, action, responseText, queue):
        """

        - move code to another function, to support easier subclassing.
        - check validity of path
        - save to file
        """
        filepath = self.validateFilePath(action, responseText)
        if filepath:
            textOutput = self._manipulateText(action, responseText)
            self.saveToFile(filepath, textOutput)

    def validateFilePath(self, action, responseText) ->Path:
        if self.isFilenameInAction(action) and self.isPathInAction(action):
            path = self.buildBasePath(action, responseText)
            filename = self.buildFilename(action, responseText)
            if self.isBasePathValid(path):
                filepath = path / filename
                if self.canOverwriteFile(filepath):
                    return filepath
                else:
                    logger.warning(f"Cannot overwrite file at {filepath}")
                    return None
            else:
                logger.warning(f"Invalid path: {path}")
                return None
        else:
            filename = action.actionKwargs.get("filename")
            path = action.actionKwargs.get('path')
            logger.warning(
                f"Expected filename and path from action, but got filename: {filename} path: {path}")
            return None

    def canOverwriteFile(self, filepath: Path)-> bool:
        # can use more complicated decision making here
        if filepath.exists():
            return False
        else:
            return True

    def isFilenameInAction(self, action):
        if action.actionKwargs.get('filename'):
            return True
        else:
            return False

    def isPathInAction(self, action):
        if action.actionKwargs.get('path'):
            return True
        else:
            return False

    def isBasePathValid(self, path: Path) -> bool:
        if path.is_dir() and path.exists():
            return True
        else:
            return False

    def buildBasePath(self, action, responseText) -> Path:
        path = Path(action.actionKwargs.get('path'))
        return path

    def buildFilename(self, action, responseText):
        # - check filename for datetime field, build filename
        filename = Path(action.actionKwargs.get('filename'))
        return filename

    def _manipulateText(self, action, text):
        # override to do custom manipulation of text.
        return text

    # def combinePathAndFilename(self, path, filename):
    #     filepath = Path(path) / Path(filename)

    #     return filepath

    def saveToFile(self, filepath: Path,  text: str):

        with open(filepath, 'wt', '\n') as file:
            file.write(text)


class AsyncHttpQueueRunner(object):

    def __init__(self):
        pass

    def execute(self, actions, connections):

        try:
            asyncio.run(self._initSession(connections, actions), debug=True)
        except concurrent.futures.CancelledError:
            logger.debug("execute: Another concurrent.futures.CancelledError")
        except Exception:
            logger.exception(
                "Unhandled exception made it all the way to 'execute'")

        # return result

    async def _initSession(self, connections, actions):
        async with aiohttp.ClientSession() as session:
            await self._doActions(session, connections, actions)

    async def _doActions(self, session, connections, actions):
        queue = asyncio.Queue()
        # we init the consumers, as the queues are empty at first,
        # they will be blocked on the main_queue.get()
        consumers = [asyncio.ensure_future(self._consumer(queue, session))
                     for _ in range(connections)]
        await self._fillQueue(queue, actions)
        # wait for all item's inside the main_queue to get task_done
        await queue.join()
        # cancel all coroutines
        for consumer_future in consumers:
            consumer_future.cancel()

    async def _consumer(self, queue, session):
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

    async def _fillQueue(self, queue, actions):
        # Add some performance tracking data to the queue
        queue.highCount = len(actions)
        queue.startTime = timer()
        queue.lastReport = timer()
        for action in actions:
            await queue.put(action)

    def action_get(self,url: str, getDict: dict = None, storeResults: bool = False, retryOnFail: bool = True, retryLimit: bool = 5, callback=None, internalParams: dict = None) -> AsyncHttpGet:
        #url, getDict=None, storeResults=False, retryOnFail=True, retryLimit=5, callback=None, **kwargs
        action = AsyncHttpGet(url, getDict, storeResults, retryOnFail, retryLimit, callback,internalParams)
        return action
