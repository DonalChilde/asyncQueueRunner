import asyncio
import copy
import csv
import json
import random
from pathlib import Path
from typing import Dict, Optional, Sequence, Type

import aiohttp


async def basic_worker(queue):
    while True:
        job = await queue.get()
        await job.do_action(queue)
        queue.task_done()


class QueueAction:
    def __init__(self, name, context=None):
        self.name: str = name
        self.context: Optional[dict] = context

    async def do_action(self, queue):
        sleep_for = random.uniform(0.05, 1.0)
        await asyncio.sleep(sleep_for)
        print(f"{self.name} Action Completed. Slept for {sleep_for}")


class QueueRunner:
    def __init__(self, worker=None):
        if worker is None:
            self.worker = basic_worker
        else:
            self.worker = worker

    async def do_queue(self, actions: Sequence[Type[QueueAction]], workers: int):
        self.queue = asyncio.Queue()
        task_workers = []
        for x in range(workers):
            task = asyncio.create_task(self.worker(self.queue))
            task_workers.append(task)
        for action in actions:
            self.queue.put_nowait(action)
        await self.queue.join()
        for task in task_workers:
            task.cancel()
        await asyncio.gather(*task_workers, return_exceptions=True)


async def http_worker(queue, session):
    while True:
        job = await queue.get()
        await job.do_action(queue=queue, session=session)
        queue.task_done()


class HttpQueueRunner:
    def __init__(self, worker=None):
        if worker is None:
            self.worker = http_worker
        else:
            self.worker = worker

    async def do_queue(self, actions: Sequence[Type[QueueAction]], workers: int):
        async with aiohttp.ClientSession() as session:
            self.queue = asyncio.Queue()
            task_workers = []
            for x in range(workers):
                task = asyncio.create_task(self.worker(self.queue, session))
                task_workers.append(task)
            for action in actions:
                self.queue.put_nowait(action)
            await self.queue.join()
            for task in task_workers:
                task.cancel()
            await asyncio.gather(*task_workers, return_exceptions=True)


class HttpAction:
    def __init__(
        self,
        method: str,
        url: str,
        request_params: dict = None,
        # store_response_text: bool = False,
        retry_on_fail: bool = True,
        retry_limit: bool = 5,
        response_handlers=None,
        internal_params: Optional[dict] = None,
        context: Optional[dict] = None,
    ):
        self.method = method
        self.url = url
        self.request_params = request_params
        # self.store_response_text = store_response_text
        self.retry_on_fail = retry_on_fail
        self.retry_limit = retry_limit
        if response_handlers is None:
            self.response_handlers = []
        else:
            self.response_handlers = response_handlers
        if internal_params is None:
            self.internal_params = {}
        else:
            self.internal_params = internal_params
        if context is None:
            self.context = {}
        else:
            self.context = context
        # self.response_text = None
        self.retry_count = 0

    async def do_action(self, queue, session: aiohttp.ClientSession):
        if self.retry_count >= self.retry_limit:
            return
        self.retry_count += 1
        try:
            async with session.request(
                self.method,
                self.url,
                params=self.request_params,
                **self.internal_params,
            ) as response:
                # response_text = await response.text()
                if response.status == 200:
                    # if self.store_response_text:
                    #     self.response_text = response_text
                    for response_handler in self.response_handlers:
                        await response_handler(
                            action=self, response=response, queue=queue
                        )
                else:
                    await self.handle_error(response, queue)

        except Exception as e:
            print(e)

    async def handle_error(self, response, queue):
        # retry for server time out errors
        print(
            f"\nError Status: {response.status}\n Response Text:{await response.text()}\n URL: {response.url}\n Internal Params: {self.internal_params} "
        )
        if response.status in (503, 504):
            if self.retry_on_fail:
                await queue.put(self)


async def print_response(action, response, queue):
    print(await response.text())


async def print_page_number(
    action: HttpAction, response: aiohttp.ClientResponse, queue: asyncio.Queue
):
    page_limit = response.headers.get("x-pages", None)
    page = action.request_params.get("page", 0)
    print(f"page {page} of {page_limit}")


async def save_response(action, response, queue):
    save_path = action.context.get("save_path", None)
    save_path_provider = action.context.get("save_path_provider", None)
    if save_path_provider is not None:
        save_path = await save_path_provider(action, response, queue)
    save_data = await response.text()
    if save_path is not None and save_data:
        save_path = Path(save_path)
        save_string(save_data, save_path)


async def save_processed_response(
    action: HttpAction, response: aiohttp.ClientResponse, queue: asyncio.Queue
):
    save_path = action.context.get("save_path", None)
    save_path_provider = action.context.get("save_path_provider", None)
    if save_path_provider is not None:
        save_path = await save_path_provider(action, response, queue)
    save_data = action.context.get("response_data", None)
    if save_path is not None and save_data:
        save_path = Path(save_path)
        save_string(save_data, save_path)


async def save_response_to_json(
    action: HttpAction, response: aiohttp.ClientResponse, queue: asyncio.Queue
):
    save_path = action.context.get("save_path", None)
    save_path_provider = action.context.get("save_path_provider", None)
    if save_path_provider is not None:
        save_path = await save_path_provider(action, response, queue)
    save_data = json.dumps(await response.json(), indent=2)
    if save_path is not None and save_data:
        save_path = Path(save_path)
        save_string(save_data, save_path)


async def save_response_to_csv(
    action: HttpAction, response: aiohttp.ClientResponse, queue: asyncio.Queue
):
    save_path = action.context.get("save_path", None)
    save_path_provider = action.context.get("save_path_provider", None)
    if save_path_provider is not None:
        save_path = await save_path_provider(action, response, queue)
    save_data = await response.json()
    if save_path is not None and save_data:
        save_path = Path(save_path)
        save_list_of_dicts(save_data, save_path)


async def process_response_to_json(action, response, queue):
    action.context["response_data"] = json.dumps(await response.json(), indent=2)


async def check_for_pages(action, response, queue):
    page = action.request_params.get("page", None)
    if page is not None and page == 1:
        page_range = response.headers.get("x-pages", None)
        if page_range is not None:
            page_range = int(page_range)
            if page_range > 1:
                for x in range(page_range):
                    if x + 1 == 1:
                        continue
                    new_action = copy.deepcopy(action)
                    new_action.request_params["page"] = x + 1
                    await queue.put(new_action)


def save_string(data: str, file_path: Path) -> bool:
    """Save a string. Makes parent directories if they don't exist.
    
    Traps all errors and prints them to std out.

    Arguments:
        data {str} -- The string to save
        file_path {Path} -- Path to the saved file.
    
    Returns:
        bool -- True if successful
    """
    try:
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True)
        with open(file_path, "w") as out_file:
            out_file.write(data)
    except Exception as e:
        print(e)
        return False
    return True


def save_list_of_dicts(data: Sequence[Dict[str, any]], file_path: Path) -> bool:
    """Save a list of dicts to csv.
    
    Arguments:
        data {Sequence[Dict[str, any]]} -- list of dicts
        file_path {Path} -- path to saved file
    
    Returns:
        bool -- True if successful.
    """
    try:
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True)
        with open(file_path, "w", encoding="utf8", newline="") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        print(e)
        return False
    return True
