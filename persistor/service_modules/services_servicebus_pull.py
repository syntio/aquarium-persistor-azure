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
The manager for the Service Bus Pull capability of the Persistor.
"""

import asyncio
import logging
from typing import Dict, List

from azure.servicebus.aio import (
    ServiceBusClient,
    Message,
)

from ..custom_exceptions.persistor_exceptions import (
    ServiceBusPullConfigurationException,
    PersistorStoreException,
)
from ..service_modules.services_http_workers import ServiceHTTPTriggerManager
from ..storage_utils.utils_storage import form_data_af_service_bus_pull


class PersistorServiceBusPullManager(ServiceHTTPTriggerManager):
    """
    Main class containing the Service Bus PULL version of the Persistor.

    :param config: Dictionary containing the necessary configuration parameters (existence of values is not checked).

    """

    def __init__(
            self,
            config: Dict,
    ):

        super().__init__(config)

        self.sb_conn_str = config["SERVICE_BUS_CONNECTION_STRING"]
        self.idle_timeout = config.get("SERVICE_BUS_IDLE_TIMEOUT", 0)

        if config["SERVICE_BUS_TYPE"] == "QUEUE":
            self.client_creator = self.create_queue_client
            self.save_param = self.queue_name = config["SERVICE_BUS_QUEUE_NAME"]
        else:
            self.client_creator = self.create_sub_client
            self.save_param = self.topic_name = config["SERVICE_BUS_TOPIC_NAME"]
            self.sub_name = config["SERVICE_BUS_SUB_NAME"]

        self.exception_type = ServiceBusPullConfigurationException
        self.receive_duration = config.get("SERVICE_BUS_RECEIVE_DURATION")
        self.prefetch_param = config["SERVICE_BUS_PREFETCH"]

        self.form_func = form_data_af_service_bus_pull

    def create_queue_client(
            self,
            sb_client: ServiceBusClient,
    ):
        """
        Creates the Queue Service Bus client.

        :param sb_client: Client for the Service Bus.
        :return: Queue Service Bus Client.
        """

        return sb_client.get_queue(self.queue_name)

    def create_sub_client(
            self,
            sb_client: ServiceBusClient,
    ):
        """
        Creates the Subscription Service Bus client.

        :param sb_client: Client for the Service Bus.
        :return: Subscription Service Bus Client.
        """

        return sb_client.get_subscription(self.topic_name, self.sub_name)

    async def store_batch(
            self,
            messages: List[Message],
            processed_messages: List[int]
    ):
        """
        Stores a batch of messages.

        :param messages: List of Service Bus messages.
        :param processed_messages: The number of processed messages.
        :return:
        """

        extracted_data = [self.get_data(m) for m in messages]
        file_name, result = await self.process_func(extracted_data)

        if not result:
            raise PersistorStoreException(
                f"FAILED TO SAVE EVENTS TO STORAGE! BLOB NAME: {file_name}"
            )

        # msg_cleared = len(messages)
        extracted_data.clear()

        while len(messages) != 0:
            message = messages.pop(0)
            try:
                await message.complete()
                processed_messages[0] += 1  # Concurrency isn't as much of an issue here; it's a simple counter.
            except Exception:
                logging.error(
                    "FAILED TO ACKNOWLEDGE MESSAGE: %s",
                    str(message)
                )

    async def pull_loop(
            self,
            receiver_client,
            processed_messages: List[int],
    ):
        """
        The specific coroutine run to process the messages.

        :param receiver_client: The receive client.
        :param processed_messages: The number of processed messages.
        :return:
        """

        data = []
        msg_num = 0
        batch = []

        try:

            while True:

                batch = await receiver_client.fetch_next(
                    max_batch_size=self.prefetch_param,
                    timeout=10,
                )

                if not batch:
                    break

                while len(batch) != 0:
                    message = batch.pop(0)

                    msg_num += 1
                    data.append(message)

                    if msg_num % self.batch_size_to_store == 0:
                        await self.store_batch(data, processed_messages)

            if data:
                await self.store_batch(data, processed_messages)

        except asyncio.CancelledError:
            if data:
                await self.store_batch(data, processed_messages)
            if batch:
                for message in batch:
                    await message.abandon()
        # There is a possibility that the receiver might simply break due to reasons beyond our control.
        # In that case, we still cannot pass the exception, as it might be the use-case where there are
        # concurrent tasks being run.
        except Exception as exc:
            logging.error(
                "ERROR OCCURRED WITH RECEIVER! | EXCEPTION BODY: %s",
                str(exc)
            )

    def receive(
            self,
            receiver_client,
            processed_messages: List[int],
            **kwargs,
    ):
        """
        Receives and processes the messages from the Service Bus.

        :param receiver_client: The receiver client.
        :param processed_messages: The number of processed messages.
        :return: Receive future.
        """

        return asyncio.ensure_future(
            self.pull_loop(
                receiver_client,
                processed_messages,
            )
        )

    async def begin_processing(
            self,
            processed_messages: List[int],
    ):
        """
        The main pull tasks.
        Envisioned to run concurrently.

        :param processed_messages: The number of processed messages.
        :return:
        """

        try:
            sb_client = ServiceBusClient.from_connection_string(self.sb_conn_str)
            client = self.client_creator(sb_client)
            receiver_client = client.get_receiver(
                idle_timeout=self.idle_timeout,
                prefetch=self.prefetch_param,
            )
        except Exception as exc:
            logging.error("UNABLE TO INSTANTIATE ONE OF THE NECESSARIY CLIENTS!")
            raise exc

        await self.time_constrained_pull(
            receiver_client,
            processed_messages,
        )
