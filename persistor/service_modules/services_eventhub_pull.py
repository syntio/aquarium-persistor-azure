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
The manager for the Event Hub Pull capability of the Persistor.
"""

import asyncio
import logging
from typing import Dict, List

from azure.eventhub import EventData
from azure.eventhub.aio import (
    EventHubConsumerClient,
    PartitionContext,
)
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

from ..custom_exceptions.persistor_exceptions import (
    EventHubPullConfigurationException,
    PersistorStoreException,
)
from ..service_modules.services_http_workers import ServiceHTTPTriggerManager
from ..storage_utils.utils_storage import form_data_af_event_hub_pull


class PersistorEventHubPullManager(ServiceHTTPTriggerManager):
    """
    Main class containing the Event Hub PULL version of the Persistor.

    :param config: Dictionary containing the necessary configuration parameters (existence of values is not checked)

    """

    def __init__(
            self,
            config: Dict,
    ):

        super().__init__(config)

        self.hub_name = config["EVENT_HUB_NAME"]
        self.hub_connection_str = config["EVENT_HUB_CONNECTION_STRING"]
        self.consumer_group = config["EVENT_HUB_CONSUMER_GROUP"]
        self.idle_timeout = config["EVENT_HUB_IDLE_TIMEOUT"]

        self.receive_duration = config["EVENT_HUB_RECEIVE_DURATION"]

        self.checkpoint_container = config["EVENT_HUB_CHECKPOINT"]
        self.checkpoint_conn_str = config["EVENT_HUB_CHECKPOINT_STORAGE_CONN_STR"]
        self.checkpoint_update_batch = config["EVENT_HUB_CHECKPOINT_UPDATE_RATE"]
        self.prefetch_param = config["EVENT_HUB_PREFETCH"]
        self.max_pull_batch_size = config["EVENT_HUB_PULL_MAX_BATCH"]

        self.save_param = self.hub_name

        self.exception_type = EventHubPullConfigurationException

        self.form_func = form_data_af_event_hub_pull

    @staticmethod
    async def on_error(
            partition_context,
            error,
    ):
        """
        Callback function, triggered on error.

        :param partition_context: Contains the client's cursor while going through a partition.
        Used to update the checkpoint blob.
        :param error: The exception that triggered the calback.
        :return:
        """

        if partition_context:
            logging.error(
                "AN UNEXPECTED ERROR HAS OCCURRED! | EXCEPTION: %s ",
                str(error),
            )
        else:
            logging.error(
                "AN ERROR OCCURRED DURING LOAD BALANCING! | EXCEPTION %s ",
                str(error),
            )

    async def store_batch(
            self,
            data: List[str],
            processed_messages: List[int],
            partition_context: PartitionContext,
            last_event: EventData,
    ):
        """
        Stores batch of events.

        :param data: List of extracted payloads from events.
        :param processed_messages: Numnber of processed messages.
        :param partition_context: The partition context.
        :param last_event: The last event to be stored (used for updating the checkpoint.)
        :return:
        """

        # Store the batch.
        file_name, result = await self.process_func(data)

        if not result:
            raise PersistorStoreException(f"FAILED TO SAVE EVENTS TO STORAGE! BLOB NAME: {file_name}")

        processed_messages[0] += len(data)

        data.clear()
        await partition_context.update_checkpoint(last_event)

    def get_callback_func(
            self,
            processed_messages: List[int],
    ):
        """
        Generates a callback function, using an overall number of processed messages for tracking.

        :param processed_messages: Counter of processed events.
        :return: Callback function used on batch.
        """

        async def on_events(
                partition_context,
                events,
        ):
            """
            Callback function to be triggered when event batch is received.
            Events are stored individually to blob storage through this function.

            :param partition_context: Contains the client's cursor while going through a partition.
            Used to update the checkpoint blob.
            :param events: The received event hub event batch.
            :return:
            """

            data = []
            last_event = None
            event_num = 0

            if not events:
                return

            for event in events:

                # Extract payload and add to the list.
                data.append(self.get_data(event))
                last_event = event
                event_num += 1

                # If batch number reached, store.
                if event_num % self.batch_size_to_store == 0:
                    await self.store_batch(
                        data,
                        processed_messages,
                        partition_context,
                        event,
                    )

            # If list done, store the leftovers.
            if data:
                await self.store_batch(
                    data,
                    processed_messages,
                    partition_context,
                    last_event,
                )

        return on_events

    def receive(
            self,
            client,
            processed_messages: List[int],
            **kwargs,
    ):
        """
        Begins the receiving of messages from EventHub.

        :param client: The EventHubConsumerClient instance that will be pulling messages for a particular task.
        :param processed_messages: Counter of processed events.
        :return: Receive function future.
        """

        return asyncio.ensure_future(
            client.receive_batch(
                on_event_batch=self.get_callback_func(
                    processed_messages,
                ),
                on_error=self.on_error,
                starting_position="-1",
                max_batch_size=self.max_pull_batch_size,
                prefetch=self.prefetch_param,
            )
        )

    async def begin_processing(
            self,
            processed_messages: List[int],
    ):
        """
        The function the concurrent tasks will be running.
        Starts the shared checkpoint blob and the Event Hub client.

        :param processed_messages: Counter of processed events.
        :return:
        """

        # Initialize the checkpoint store mechanism.
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            self.checkpoint_conn_str,
            self.checkpoint_container,
        )

        # Initialize the asynchronous Consumer Client.
        client = EventHubConsumerClient.from_connection_string(
            conn_str=self.hub_connection_str,
            consumer_group=self.consumer_group,
            eventhub_name=self.hub_name,
            checkpoint_store=checkpoint_store,
            idle_timeout=self.idle_timeout,
        )

        # Begin the pull.
        # (See parent class.)
        await self.time_constrained_pull(
            client,
            processed_messages,
        )
