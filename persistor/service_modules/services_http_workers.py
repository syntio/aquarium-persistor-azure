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
Class used by our PULL methods (HTTP-triggered)
"""

import asyncio
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from azure.functions import HttpRequest

from ..custom_exceptions.persistor_exceptions import PeristorHTTPTriggerConfigurationException
from ..service_modules.services_manager_base import ServiceManagerBase
from ..storage_utils.append_control import generate_append_blob
from ..storage_utils.utils_storage import (
    save_to_storage,
    form_blob_json_string,
)


class ServiceHTTPTriggerManager(ServiceManagerBase):

    """
    Base class used for services that use the PULL functionality, triggered by an HTTP call.

    :param config: Dictionary containing the necessary configuration parameters (existence of values is not checked).

    """

    # The amount of seconds the update_time function will take between checking the current time
    TIME_UPDATE_RATE = 5

    # Number of messages to be stored at a time
    DEFAULT_BATCH_MESSAGES_TO_STORE = 200

    # Max possible batch size
    MAX_ALLOWED_BATCH_SIZE = 10000

    # HTTP request containing integer parameters
    HTTP_REQ_INT_PARAMS = [
        ("N", "TASK_NUM", 1, "NUMBER OF CONCURRENT TASKS"),
        ("batch_store_size", "BATCH_SIZE", DEFAULT_BATCH_MESSAGES_TO_STORE, "MESSAGES TO STORE IN A BATCH"),
    ]

    def __init__(
            self,
            config: Dict,
    ):

        super().__init__(config)

        if self.output_binding:
            raise PeristorHTTPTriggerConfigurationException(
                "OUTPUT BINDING SHOULD NOT BE ENABLED FOR HTTP/PULL VERSIONS OF THE PERSISTOR!"
            )

        if not self.container_client:
            raise PeristorHTTPTriggerConfigurationException(
                "INVALID CONFIGURATION! (SETTINGS FAILED TO CREATE A CONTAINER CLIENT!)"
            )

        self.process_func = self.store_with_client
        self.form_func = None

        self.returnable = True
        self.save_param = None
        self.exception_type = PeristorHTTPTriggerConfigurationException

        self.file_name = None

        self.receive_duration = None

        self.form_func = None

        self.prefetch_param = 1
        self.batch_size_to_store = ServiceHTTPTriggerManager.DEFAULT_BATCH_MESSAGES_TO_STORE

    @abstractmethod
    async def begin_processing(
            self,
            processed_messages: List[int],
    ):
        """
        The main pull tasks.
        Envisioned to run concurrently.

        :param processed_messages: Counter of processed events. Designed as a list to make the integer value mutable.
        :return:
        """

    @abstractmethod
    def receive(
            self,
            client,
            processed_messages: List[int],
            **kwargs,
    ):
        """
        Begins the receiving process.

        :param client: Main client/receiver designated to receive and process messages/events.
        :param processed_messages: Number of processed messages. Designed as a list to make the integer value mutable.
        :return: Receive task future.
        """

    def get_data(
            self,
            message,
    ):
        """
        Extract the JSON payload from a message, along with (optionally) metadata.

        :param message: The message to be processed.
        :return:
        """

        return form_blob_json_string(
            msg=message,
            get_metadata=self.get_metadata,
            form_func=self.form_func,
        )

    async def store_with_client(
            self,
            extracted_data: List[str],
    ):
        """
        Main method used for storing with the asyncronous blob client.

        :param extracted_data: Events or messages to store.
        :return: File name and success of storing.
        """

        return await save_to_storage(
            data=extracted_data,
            container_client=self.container_client,
            store_param=self.save_param,
            append=self.append,
            file_append_name=self.file_name,
        )

    async def time_constrained_pull(
            self,
            client,
            processed_messages: List[int],
            **kwargs,
    ):
        """
        Before starting the receive, ensure that it is finished within the specified time duration (if any set).

        :param client: Main client/receiver designated to receive and process messages/events.
        :param processed_messages: Counter of processed events.
        :return:
        """

        if self.receive_duration:

            # Async with will ensure the client is closed the moment we leave this block.
            async with client:

                # Run the task, keeping the record of the task itself.
                task = self.receive(
                        client,
                        processed_messages,
                        **kwargs,
                )

                # Wait for the specified duration of the pull.
                await asyncio.sleep(self.receive_duration)

                task.cancel()

        else:
            # If no duration is set, pull indefinitely.
            task = self.receive(
                client,
                processed_messages,
                **kwargs,
            )
            await task

    async def update_time(
            self,
            finished_process: List[bool]
    ):
        """
        If the timed append fature is used, this function keeps track of whether or not time has changed
        and updates the destination blob accordingly.

        :param finished_process: Boolean flag determining whether or not the Persistor processes have finished.
        :return:
        """

        # While the other tasks are running, keep track of the changes in time.
        while not finished_process[0]:

            await generate_append_blob(
                container_client=self.container_client,
                manager=self,
                store_param=self.save_param,
                time_based=self.timed_append,
            )

            # Wait 5 seconds until next update.
            await asyncio.sleep(ServiceHTTPTriggerManager.TIME_UPDATE_RATE)

    async def process_controller(
            self,
            task_num: int,
            finished_process: List[bool]
    ):
        """
        Runs the concurrent Persistor methods.
        Sets the internal flag of finishing upon complention, in order to disable the
        method for updating time.

        :param task_num: Number of concurrent tasks.
        :param finished_process: Boolean keeping track of whether or not the Persistor processes have finished.
        :return:
        """

        processed_messages = [0]
        await asyncio.gather(
            *[self.begin_processing(processed_messages) for _ in range(task_num)]
        )

        # Once all the tasks are complete, mark the processes as finished.
        finished_process[0] = True

        return processed_messages

    async def run_persistor(
            self,
            req: HttpRequest,
            **kwargs,
    ):
        """
        Main initialization method shared by all of the PULL implementations.

        :param req: The HTTP request that triggered the function.
        :return: Number of stored messages.
        """

        # Get the number of concurrent tasks to be run and raise the appropriate exception type if needed,
        # depending on the Persistor type.
        req = req.params
        req_configs = {}

        # Load the integer parameters
        for req_key, config_key, default_val, desc in ServiceHTTPTriggerManager.HTTP_REQ_INT_PARAMS:
            try:
                req_configs[config_key] = int(req.get(req_key, default_val))
            except TypeError as exc:
                raise self.exception_type(
                    f"INVALID VALUE FOR {desc}!"
                ) from exc

        task_num = req_configs["TASK_NUM"]
        self.batch_size_to_store = min(
            req_configs["BATCH_SIZE"],
            ServiceHTTPTriggerManager.MAX_ALLOWED_BATCH_SIZE - self.prefetch_param,
        )

        # Configure the thread pool executor, unbinding the maximum number of workers
        loop = asyncio.get_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=100))

        # Boolean flag to track whether the overall Persistor process has concluded.
        # We do not keep this as a class variable, as it is possible that Azure Functions place two functions
        # in the same process; this would cause major concurrency issues if one triggered just slightly after
        # the other.
        finished_process = [False]

        # Coroutines to be run.
        coroutines = []

        # Generate the file name at the start if the append option is enabled.
        if self.append:
            self.file_name = await generate_append_blob(
                container_client=self.container_client,
                manager=self,
                store_param=self.save_param,
                time_based=self.timed_append,
            )

            # The other "main" coroutine is responsible for initiating the coroutine
            # in charge of updating current time, and with it, the destination append blob.
            if self.timed_append:
                coroutines.append(self.update_time(finished_process))

        # One of the "main" coroutines is responsible for initiating the N
        # Pull tasks.
        coroutines.append(self.process_controller(task_num, finished_process))

        # Wait for the coroutines to finish.
        results = await asyncio.gather(*coroutines)

        # Get the number of processed messages.
        results = results[1] if self.timed_append else results[0]

        return str(results[0])
