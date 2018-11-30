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




"""


from timeit import default_timer as timer
#from time import perf_counter_ns as timer
import asyncio
import aiohttp
import logging
import uuid
import concurrent.futures
from asyncQueueRunner.asyncQueueRunner import AsyncAction, ActionStatus


MAX_CONSUMERS = 100
MAX_QUEUE_SIZE = 0


# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console = logging.StreamHandler()
# logger.addHandler(console)


class AsyncHttpGet(object):
    """Holds info for an Http Get action, and data from its Response.

    This class holds the info required for a Get action, and can store
    the info from a response. Typical use is to make a list of these actions
    by supplying a URL, giving that list to a QueueRunner.execute, and then 
    manipulating the returned data. 
    """

    def __init__(self, url,params = None, responseHandler=None, retryLimit=5):
        #TODO enable default response handler

        #super().__init__(actionHandler=actionHandler, retryLimit=retryLimit)
        self.url = url
        self.params = params
        self.uuid = uuid.uuid4()
        self.retryCounter = 0
        self.retryLimit = retryLimit
        self.responseHandler = responseHandler
        self.completedActionStatus = None
        self.completedActionStatusMessage = None
        self.completedActionData = None
        self.responseUrl = None
        self.startTime = 0
        self.endTime = 0

    def __repr__(self):
        return (f'<{self.__class__.__name__}('
               f'url={self.url!r}, params={self.params!r},'
               f'responseHandler={self.responseHandler}, retryLimit={self.retryLimit})>')

    async def doAsyncAction(self, queue, session):
        self.retryCounter += 1
        try:
            async with session.get(self.url, params=self.params) as response:
                result = await self.responseHandler.handleResponse(
                    self, response, queue)
                return result
        except asyncio.TimeoutError as e:
            logger.exception(e)

        except aiohttp.ClientConnectionError as e:
            logger.exception(f"Connection error in {self} ")
            raise e

        except aiohttp.ClientError as e:
            logger.exception(e)

    def elapsedTime(self):
        return f"{self.endTime-self.startTime:.3f}s"


class AsyncHttpGetResponseHandler(object):
    """handle the results of an http get

    This is where most of the custom work gets done.
    In most cases, this is the only class that will need
    to be customized.

    The workflow feels kind of clunky, needs to be improved.
    This class should be able to
    """

    def __init__(self, storeResults=False):
        # super().__init__()
        self.storeResults = storeResults
        #self.storedResults = None
    def __repr__(self):
        return (f'<{self.__class__.__name__}('
               f'storeResults={self.storeResults!r})>')

    async def handleResponse(self, action, response, queue):
        """


        """
        responseStatus = response.status
        responseReason = response.reason
        responseUrl = response.url
        responseText = await response.text()
        self._storeResponse(action, responseStatus,
                            responseReason, responseUrl,responseText)
        await self._manipulateResponseText(action, responseText, queue)
        await self._checkForRetry(action, queue, responseStatus, responseReason, responseText)

    async def _manipulateResponseText(self, action, responseText, queue):
        """override this method to provide custom handling of response text,
        like saving to a file or database, or changing text to JSON. Good for 
        handling large amounts of data from large numbers of requests.
        """
        pass

    def _storeResponse(self, action, responseStatus, responseReason, responseUrl,responseText):
        action.completedActionStatus = responseStatus
        action.completedActionStatusMessage = responseReason
        action.responseUrl = responseUrl
        if self.storeResults:
            action.completedActionData = responseText

    async def _checkForRetry(self, action, queue, responseStatus, responseReason, responseText):
        if responseStatus == 200:  # ActionStatus.SUCCESS
            return
        if responseStatus == 404:  # ActionStatus.FAIL_NO_RETRY
            return
        if responseStatus == "FOO":  # ActionStatus.RETRY
            await queue.put(action)
            return
        logger.info(
            f"Action: {action} recieved unhandled response status of {responseStatus}: {responseReason}")
        # TODO remove this after finding the most common retry senarios
        raise NotImplementedError(f"Status Code: {responseStatus}")
        # return


class AsyncHttpQueueRunner(object):

    def __init__(self):
        pass

    

    def execute(self, actions, connections):
        
        
        try:
            asyncio.run(self._initSession(connections, actions),debug=True)
        except concurrent.futures.CancelledError:
            logger.debug("execute: Another concurrent.futures.CancelledError")
        except Exception:
            logger.exception(
                "Unhandled exception made it all the way to 'execute'")
    
        # return result
    
    
    async def _initSession(self, connections, actions):
        async with aiohttp.ClientSession() as session:
            result = await self._doActions(session, connections, actions)
    
    
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
            #start = timer()
            action.startTime = timer()
            try:
                if action.retryLimit == -1 or action.retryCounter <= action.retryLimit:
                    result = await action.doAsyncAction(queue, session)
                    # await self.handleResult(result, queue)
                    #action.endTime = timer()
                    queue.task_done()
                else:
                    #action.endTime = timer()
                    queue.task_done()
                    continue
            except aiohttp.ClientConnectorError as e:
                #Unable to connect to Host - No internet?
                #Signals task done to queue, tries to pass exception up the chain,
                #but it never leaves the loop?
                queue.task_done()
                raise e
    
            except Exception as e:
                logger.exception(e)

            finally:
                action.endTime = timer()
    
            #end = timer()
    
            time_to_complete = action.endTime - action.startTime
            logging.info(f"Time to complete action: {time_to_complete:.3f}s")
    
    
    async def _fillQueue(self, queue, actions):
        # Add some performance tracking data to the queue
        queue.highCount = len(actions)
        queue.startTime = timer()
        queue.lastReport = timer()
        for action in actions:
            await queue.put(action)


# ------ Previous code ---------


# class HttpQueueRunner(object):
#     """Runner for executing async actions.

#     """

#     def __init__(self, asyncActions, asyncActionConsumer=None):
#         self.actions = asyncActions
#         if asyncActionConsumer == None:
#             self.consumerProvider = AsyncHttpActionConsumer()
#         else:
#             self.consumerProvider = asyncActionConsumer

#     def execute(self, consumerLimit=0):

#         if consumerLimit > 0:
#             max_consumers = min(MAX_CONSUMERS, consumerLimit)
#         else:
#             max_consumers = MAX_CONSUMERS

#         event_loop = asyncio.get_event_loop()
#         try:

#             event_loop.run_until_complete(
#                     self._doActions(max_consumers, self.actions))
#         except concurrent.futures.CancelledError as e:
#             logger.debug("execute: Another concurrent.futures.CancelledError")
#         except Exception as e:
#             logger.info(e)

#     async def _doActions(self, max_consumers, actions):
#         queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

#         # Init consumers. They will be blocked on the main_queue.get()
#         # because the queue is empty.
#         async with aiohttp.ClientSession() as session:
#             consumers = [asyncio.ensure_future(self.consumerProvider.consumer(queue,session))
#                      for _ in range(max_consumers)]

#             await self._fillQueue(queue, actions)
#         # wait for all item's inside the queue to get task_done
#             await queue.join()
#         # cancel all coroutines
#             for consumer_future in consumers:
#                 consumer_future.cancel()

#     async def _fillQueue(self, queue, actions):
#         # Add some performance tracking data to the queue
#         queue.highCount = len(actions)
#         queue.startTime = timer()
#         queue.lastReport = timer()
#         for action in actions:
#             await queue.put(action)

# class AsyncHttpActionConsumer(object):
#     """
#     """
#     # TODO figure out a backoff system

#     def __init__(self):
#         pass

#     async def consumer(self, queue, session):
#         while True:
#             action = await queue.get()
#             start = timer()
#             try:
#                 if action.retryLimit == -1 | action.retryCounter <= action.retryLimit:
#                     result = await action.doAsyncAction(session)
#                     await self.handleResult(result, queue)
#                     queue.task_done()
#                 else:
#                     queue.task_done()
#                     continue

#             except Exception as e:
#                 print(e)

#             end = timer()

#             time_to_complete = end - start
#             # TODO logging
#             print(time_to_complete)

#     async def handleResult(self, result, queue):
#         resultCode, action = result
#         if resultCode == ActionStatus.SUCCESS:
#             pass
#         elif resultCode == ActionStatus.RETRY:
#             await queue.put(action)
#             queue.highCount += 1
#         elif resultCode == ActionStatus.FAIL_NO_RETRY:
#             pass
#         elif resultCode == ActionStatus.ADD_ACTIONS:
#             # TODO test for list to avoid errors
#             for item in action:
#                 await queue.put(item)
#             queue.highCount += len(action)


# class AsyncHttpGet(AsyncAction):
#     """
#     """

#     def __init___(self, url="", actionHandler=None, retryLimit=5):

#         super().__init__(actionHandler=actionHandler, retryLimit=retryLimit)
#         self.url = url

#     async def doAsyncAction(self, session):

#         try:
#             url2 = self.kwargs['url']
#             print(url2)
#             async with session.get(url2) as response:

#                 result = await self.actionHandler.handleActionResults(
#                         self, response)
#                 return result
#         except asyncio.TimeoutError as e:
#             pass

#         except aiohttp.ClientError as e:
#             pass


# class AsyncHttpGetHandler(object):
#     """handle the results of an http get

#     This is where most of the custom work gets done.
#     In most cases, this is the only class that will need
#     to be customized.

#     The workflow feels kind of clunky, needs to be improved.
#     This class should be able to
#     """

#     def __init__(self, storeResults=False):
#         super().__init__()
#         self.storeResults = storeResults
#         if storeResults == False:
#             self.storedResults = None
#         else:
#             self.storedResults = []

#     async def handleActionResults(self, action, actionResult):
#         """


#         """
#         # Check to see if the action status can be determined from
#         # aiohttp.response.status
#         actionStatus = self.actionStatusFromResponseStatus(actionResult)
#         if actionStatus != None:
#             if self.storeResults:
#                 self.storedResults.append(
#                     (actionStatus, action, actionResult, None),)
#             return actionStatus, action

#         actionStatus, manipulatedActionResult = self.manipulateActionResults(
#             action, actionResult)

#         if self.storeResults:
#             self.storedResults.append(
#                 (actionStatus, action, actionResult, manipulatedActionResult))
#         return actionStatus, action

#     async def manipulateActionResults(self, action, actionResult):
#         manipulatedActionResult = None
#         #
#         # ...Do work on actionResult here...
#         #

#         actionStatus = ActionStatus.SUCCESS
#         return actionStatus, manipulatedActionResult

#     def actionStatusFromResponseStatus(self, response):
#         responseStatus = response.status
#         if responseStatus == 200:
#             return None
#         if responseStatus == 404:
#             return ActionStatus.FAIL_NO_RETRY

#         raise NotImplementedError(f"Status Code: {responseStatus}")
