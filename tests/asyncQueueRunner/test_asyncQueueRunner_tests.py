import random
import time
import asyncio
from asyncQueueRunner.asyncQueueRunner import AsyncAction, QueueRunner, ActionStatus

class PrintWithDelay(AsyncAction):
    def __init__(self, counter):
        super().__init__()
        self.counter = counter
        

    async def doAction(self):
        sleepDuration = random.randint(0,5)
        await asyncio.sleep(sleepDuration)
        print(f"Number {self.counter} printed after a delay of {sleepDuration} seconds")
        return ActionStatus.SUCCESS, self
        
def test_asyncQueue(capsys):

    actions = [PrintWithDelay(x) for x in range(20)]
    runner = QueueRunner(actions)
    try:
        runner.execute(0)
    except Exception as e:
        pass
        # tell pytest to print some diagnostic info
        # this does not print the stack trace, and I dont know why.
        # with capsys.disabled():
        #     print("\n----exception:---- \n")
        #     print(e)
        #     print("\n----stdout:---- \n")
        #     print(capsys.readouterr().out)
        #     print("\n----stderr:---- \n")
        #     print(capsys.readouterr().err)
    assert(False)

def test_print():
    assert(1==1)
    print("/n/ntest print/n/n")