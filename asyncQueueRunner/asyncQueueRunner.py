'''

'''
import asyncio
import logging
import concurrent.futures
from timeit import default_timer as timer
from enum import Enum, auto

# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console = logging.StreamHandler()
# logger.addHandler(console)

# Constants
# TODO move these to arguments of QueueRunner
MAX_CONSUMERS = 1000
MAX_QUEUE_SIZE = 0
CONSUMER_TIMEOUT = 10


class ActionStatus(Enum):
    SUCCESS = 1
    RETRY = 2
    FAIL_NO_RETRY = 3
    ADD_ACTIONS = 4


class QueueRunner(object):
    """Runner for executing async actions.

    """

    def __init__(self, asyncActions, asyncActionConsumer=None):
        self.actions = asyncActions
        if asyncActionConsumer == None:
            self.consumerProvider = AsyncActionConsumer()
        else:
            self.consumerProvider = asyncActionConsumer

    def execute(self, consumerLimit=0, **kwargs):

        if consumerLimit > 0:
            max_consumers = min(MAX_CONSUMERS, consumerLimit)
        else:
            max_consumers = MAX_CONSUMERS

        event_loop = asyncio.get_event_loop()
        try:
            event_loop.run_until_complete(
                self._doActions(max_consumers, self.actions))
        except concurrent.futures.CancelledError as e:
            logger.debug("execute: Another concurrent.futures.CancelledError")
        except Exception as e:
            logger.info(e)

    async def _doActions(self, max_consumers, actions, **kwargs):
        queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

        # Init consumers. They will be blocked on the main_queue.get()
        # because the queue is empty.
        consumers = [asyncio.ensure_future(self.consumerProvider.consumer(queue, **kwargs))
                     for _ in range(max_consumers)]

        await self._fillQueue(queue, actions)
        # wait for all item's inside the queue to get task_done
        await queue.join()
        # cancel all coroutines
        for consumer_future in consumers:
            consumer_future.cancel()

    async def _fillQueue(self, queue, actions):
        # Add some performance tracking data to the queue
        queue.highCount = len(actions)
        queue.startTime = timer()
        queue.lastReport = timer()
        for action in actions:
            await queue.put(action)


class AsyncAction(object):
    """Represents an async action. Inherit and override doAction().
    """

    def __init__(self, actionHandler=None, retryLimit=-1, **kwargs):
        self.retryCounter = 0
        self.retryLimit = retryLimit
        self.kwargs = kwargs
        if actionHandler == None:
            self.actionHandler = AsyncActionHandler()
        else:
            self.actionHandler = actionHandler
        #self.uuid = None

    async def doAction(self):
        self.retryCounter += 1
        x = 1+1
        handledResults = self.actionHandler.handleActionResults(
            self, x, **self.kwargs)
        return handledResults




class AsyncActionHandler(object):
    def __init__(self):
        pass

    async def handleActionResults(self, action, actionResult, **kwargs):
        # do things to evaluate the results of the action
        # decide what to do with the answers.
        return ActionStatus.SUCCESS, action


class AsyncActionConsumer(object):
    """
    """
    # TODO figure out a backoff system

    def __init__(self):
        pass

    async def consumer(self, queue, **kwargs):
        while True:
            action = await queue.get()
            start = timer()
            try:
                if action.retryLimit == -1 | action.retryCounter <= action.retryLimit:
                    result = await action.doAction(**kwargs)
                    await self.handleResult(result, queue)
                    queue.task_done()
                else:
                    queue.task_done()
                    continue

            except Exception as e:
                print(e)

            end = timer()

            time_to_complete = end - start
            # TODO logging
            print(time_to_complete)

    async def handleResult(self, result, queue):
        resultCode, action = result
        if resultCode == ActionStatus.SUCCESS:
            pass
        elif resultCode == ActionStatus.RETRY:
            await queue.put(action)
            queue.highCount += 1
        elif resultCode == ActionStatus.FAIL_NO_RETRY:
            pass
        elif resultCode == ActionStatus.ADD_ACTIONS:
            # TODO test for list to avoid errors
            for item in action:
                await queue.put(item)
            queue.highCount += len(action)
