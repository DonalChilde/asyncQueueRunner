import asyncio
import csv
import json
from pathlib import Path

import aiohttp
import pytest

from asyncQueueRunner.async_http_get import (
    HttpAction,
    HttpQueueRunner,
    QueueAction,
    QueueRunner,
    basic_worker,
    check_for_pages,
    print_page_number,
    print_response,
    process_response_to_json,
    save_processed_response,
    save_response,
    save_response_to_csv,
    save_response_to_json,
)


def test_basic_runner():
    actions = []
    for x in range(20):
        action = QueueAction(x)
        actions.append(action)

    runner = QueueRunner(basic_worker)
    asyncio.run(runner.do_queue(actions, 5))


def test_html_get():
    actions = []
    url = "https://esi.evetech.net/v1/universe/regions/"
    request_params = {"datasource": "tranquility"}
    action = HttpAction(
        method="GET",
        url=url,
        request_params=request_params,
        # store_response_text=True,
        response_handlers=[
            print_response,
            process_response_to_json,
            save_processed_response,
        ],
        context={},
    )
    action.context["save_path"] = Path("~/tmp/region_ids.json").expanduser()
    actions.append(action)
    runner = HttpQueueRunner()
    asyncio.run(runner.do_queue(actions, 5))
    # print(action.response_text)


def test_get_region_names():
    region_ids = [
        10000001,
        10000002,
        10000003,
        10000004,
        10000005,
        10000006,
        10000007,
        10000008,
        10000009,
        10000010,
        10000011,
        10000012,
        10000013,
        10000014,
        10000015,
        10000016,
        10000017,
        10000018,
        10000019,
        10000020,
        10000021,
        10000022,
        10000023,
        10000025,
        10000027,
        10000028,
        10000029,
        10000030,
        10000031,
        10000032,
        10000033,
        10000034,
        10000035,
        10000036,
        10000037,
        10000038,
        10000039,
        10000040,
        10000041,
        10000042,
        10000043,
        10000044,
        10000045,
        10000046,
        10000047,
        10000048,
        10000049,
        10000050,
        10000051,
        10000052,
        10000053,
        10000054,
        10000055,
        10000056,
        10000057,
        10000058,
        10000059,
        10000060,
        10000061,
        10000062,
        10000063,
        10000064,
        10000065,
        10000066,
        10000067,
        10000068,
        10000069,
        11000001,
        11000002,
        11000003,
        11000004,
        11000005,
        11000006,
        11000007,
        11000008,
        11000009,
        11000010,
        11000011,
        11000012,
        11000013,
        11000014,
        11000015,
        11000016,
        11000017,
        11000018,
        11000019,
        11000020,
        11000021,
        11000022,
        11000023,
        11000024,
        11000025,
        11000026,
        11000027,
        11000028,
        11000029,
        11000030,
        11000031,
        11000032,
        11000033,
        12000001,
        12000002,
        12000003,
        12000004,
        12000005,
        13000001,
    ]

    actions = []
    server_url = "https://esi.evetech.net"
    api_url = "/v3/universe/names/"
    request_params = {"datasource": "tranquility"}
    internal_params = {"json": region_ids}
    action = HttpAction(
        method="POST",
        url=server_url + api_url,
        request_params=request_params,
        response_handlers=[print_response, save_response_to_json],
        internal_params=internal_params,
    )
    action.context["save_path"] = Path("~/tmp/region_names.json").expanduser()
    actions.append(action)
    runner = HttpQueueRunner()
    asyncio.run(runner.do_queue(actions, 5))


def test_get_region_market_history():
    actions = []
    server_url = "https://esi.evetech.net"
    region_id = "10000033"
    type_id = 34
    api_url = f"/v1/markets/{region_id}/history/"
    request_params = {"datasource": "tranquility", "type_id": type_id}
    action = HttpAction(
        method="GET",
        url=server_url + api_url,
        request_params=request_params,
        response_handlers=[print_response, save_response],
    )
    action.context["save_path"] = Path(
        f"~/tmp/market_history_{region_id}_{type_id}_.json"
    ).expanduser()
    actions.append(action)
    runner = HttpQueueRunner()
    asyncio.run(runner.do_queue(actions, 5))


def test_get_region_market_history_from_file():
    actions = []
    server_url = "https://esi.evetech.net"
    region_ids = []
    type_ids = []
    market_hub_path = Path(
        "/home/chad/projects/asyncQueueRunner/tests/resources/market_hub_region_ids.csv"
    )
    type_id_path = Path(
        "/home/chad/projects/asyncQueueRunner/tests/resources/type_ids.csv"
    )
    with open(market_hub_path, newline="") as market_file:
        region_ids = list(csv.reader(market_file))
    with open(type_id_path, newline="") as type_id_file:
        type_ids = list(csv.reader(type_id_file))

    for region_id in region_ids:
        for type_id in type_ids[0:100]:
            api_url = f"/v1/markets/{region_id[0]}/history/"
            request_params = {"datasource": "tranquility", "type_id": type_id[0]}
            action = HttpAction(
                method="GET",
                url=server_url + api_url,
                request_params=request_params,
                response_handlers=[print_response, save_response],
            )
            action.context["save_path"] = Path(
                f"~/tmp/market_history_{region_id[0]}_{type_id[0]}_.json"
            ).expanduser()
            actions.append(action)
    runner = HttpQueueRunner()
    asyncio.run(runner.do_queue(actions, 5))


def test_get_region_orders():
    actions = []
    server_url = "https://esi.evetech.net"
    region_id = "10000033"
    api_url = f"/v1/markets/{region_id}/orders/"
    request_params = {"datasource": "tranquility", "page": 1, "order_type": "all"}
    action = HttpAction(
        method="GET",
        url=server_url + api_url,
        request_params=request_params,
        response_handlers=[
            check_for_pages,
            print_page_number,
            # print_response,
            # response_to_json,
            # save_response,
            save_response_to_csv,
        ],
    )
    # action.context["save_path"] = Path(
    #     f"~/tmp/market_orders_{region_id}.csv"
    # ).expanduser()
    action.context["region_id"] = region_id
    action.context["save_path_provider"] = region_order_save_path
    actions.append(action)
    runner = HttpQueueRunner()
    asyncio.run(runner.do_queue(actions, 5))


async def region_order_save_path(action: HttpAction, response, queue):
    region_id = action.context.get("region_id", 0)
    page = action.request_params.get("page", 0)
    page_limit = response.headers.get("x-pages", 0)
    save_path = Path(
        f"~/tmp/market_orders_{region_id}.{page}_of_{page_limit}.csv"
    ).expanduser()
    return save_path
