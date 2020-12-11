# Copyright 2020 Syntio Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#      http://www.apache.org/licenses/LICENSE-2.0

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Entry point for the Invoker Azure Function.
"""

import asyncio
import functools
import json
import os
from concurrent.futures import ThreadPoolExecutor

import requests

from ..custom_exceptions.persistor_exceptions import InvokerException

HTTP_FUNC = os.getenv("FUNC_TO_INVOKE")
DELAY_BETWEEN_FUNCS = float(os.getenv("DELAY_BETWEEN_FUNCS", "0.25"))
MAX_EXECUTOR_WORKERS = 32
INVOKER_TIMEOUT = float(os.getenv("INVOKER_TIMEOUT", "180"))


async def invoke_func(
        req_params,
        executor=None
):
    """
    Invokes the HTTP function. Meant to be run in parallel.

    :param req_params: Dictionary containing the parameters to be passed in the HTTP request.
    :param executor: The executor to run everything in.
    :return:
    """

    # One might wonder why we don't use something like httpx's asynchronous client
    # for these calls. When testing with httpx's async client, we ran into an issue
    # of the invoker calling double the number of requested Azure Functions.
    # This way, however, appears to seemingly work just fine.
    # In addition, we don't check/log the responses, for the simple reason that
    # the status of the called functions will inevitably be logged in App Insights.
    loop = asyncio.get_event_loop()
    try:
        res = await loop.run_in_executor(
            executor,
            functools.partial(
                requests.get,
                url=HTTP_FUNC,
                params=req_params,
                timeout=INVOKER_TIMEOUT,
            )
        )
    except requests.exceptions.Timeout:
        # A timeout is not an issue as far as the invoker is concerned, given the unpredictable
        # cold starts. It merely exists to ensure its invocations run smoothly.
        pass

    return res.text

async def main(
        req,
):
    """
    Main function to be run when the HTTP trigger is called.
    Invokes another HTTP function set in App Configuration N times concurrently.

    :param req: Must contain parameter 'N' in the request, which signifies how many times
    the invoker will invoke the HTTP Pull function.
    :return: HTTP response string.
    """

    if not HTTP_FUNC:
        raise InvokerException("NO HTTP FUNCTION LINK GIVEN TO INVOKE!")

    req = req.params

    # Get the number of parallel Azure Functions to run.
    parallel_azure_funcs = req.get("N")
    try:
        parallel_azure_funcs = int(parallel_azure_funcs)
    except TypeError as exc:
        raise InvokerException(
            "PARAMETER SPECIFIYING THE NUMBER OF FUNCTIONS TO CALL NOT SPECIFIED OR NOT AN INTEGER!"
        ) from exc

    # Get the number of concurrent tasks to run in each of the functions.
    parallel_tasks_per_func = req.get("N_per_func")
    if not parallel_tasks_per_func:
        parallel_tasks_per_func = "1"
    else:
        try:
            int(parallel_tasks_per_func)
        except TypeError as exc:
            raise InvokerException(
                "PARAMETER SPECIFYING THE NUMBER OF TASKS TO BE RUN PER FUNCTION NOT AN INTEGER!"
            ) from exc

    # Get the size of the chunks for the function to store the data in.
    batch_size = req.get("batch_store_size")
    if not batch_size:
        batch_size = ""
    else:
        try:
            int(batch_size)
        except TypeError as exc:
            raise InvokerException(
                "PARAMETER SPECIFYING THE SIZE OF BATCHES TO STORE NOT AN INTEGER!"
            ) from exc

    func_params = {
        "N": parallel_tasks_per_func,
        "batch_store_size": batch_size,
    }

    executor = ThreadPoolExecutor(
        max_workers=min(
            MAX_EXECUTOR_WORKERS,
            max(
                parallel_azure_funcs,
                (os.cpu_count() or 1) + 4,
            )
        )
    )

    coroutine_calls = []

    for _ in range(parallel_azure_funcs):
        coroutine_calls.append(invoke_func(func_params, executor))
        await asyncio.sleep(DELAY_BETWEEN_FUNCS)

    results = await asyncio.gather(*coroutine_calls)

    # Return information on each initiated HTTP Pull function's performance.
    return json.dumps({str(i): results[i] for i in range(len(results))})
