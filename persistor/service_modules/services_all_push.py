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
The manager for all of the Push/Binding capabilities of the Persistor.
"""

from typing import Dict, Optional

from azure.functions import Out

from ..custom_exceptions.persistor_exceptions import PersistorStoreException
from ..service_modules.services_manager_base import ServiceManagerBase
from ..storage_utils.append_control import generate_append_blob
from ..storage_utils.utils_storage import (
    save_to_storage,
    form_blob_json_string,
    form_data_af_event_grid,
    form_data_af_event_hub_push,
    form_data_af_service_bus_push,
)


class PersistorPushManager(ServiceManagerBase):
    """
    Main class containing all of the Azure Function Bindings (PUSH) variants of the Persistor.

    :param config: Dictionary containing the necessary configuration parameters (existence of values is not checked)

    """

    # The functions used to process the received event/message object and extract the
    # payload/metadata
    FORM_STORES = {
        "EVENT_GRID": form_data_af_event_grid,
        "EVENT_HUB": form_data_af_event_hub_push,
        "SERVICE_BUS": form_data_af_service_bus_push
    }

    # The messaging services where using the append variant immediately means it is the
    # "timed append" variant (as it is the only possible option for using the append blobs).
    AUTO_TIMED_APPENDS = [
        "EVENT_GRID",
        "SERVICE_BUS",
        "EVENT_HUB",
    ]

    def __init__(
            self,
            config: Dict,
    ):

        super().__init__(config)

        self.form_func = self.FORM_STORES[config["SERVICE"]]

        # If the function is constructed as an output binding, set the proper callback function that
        # uses the output binding blob to write the events/messages to.
        if self.output_binding:
            async def process_single_with_output_blob(messages, **kwargs):
                await self.process_messages_output_blob_binding(messages, kwargs.get("output_blob"))
            self.process_func = process_single_with_output_blob
        # Otherwise, use the callbacks that utilize the Blob Client module.
        else:
            # If no main folder was defined in case of the Event Grid, set it to the subscription ID.
            if config["SERVICE"] == "EVENT_GRID" and not config["STORE_PARAM"]:
                async def process_single_event_grid(event, **kwargs):
                    await self.process_messages(event, event.topic.split("/")[2])
                self.process_func = process_single_event_grid
            # Otherwise, use the default message storing option.
            else:
                self.store_param = func_store_param = config["STORE_PARAM"]
                async def process_all(msg, **kwargs):
                    await self.process_messages(msg, func_store_param)
                self.process_func = process_all

            if config["SERVICE"] in self.AUTO_TIMED_APPENDS:
                self.timed_append = self.append

        self.returnable = False

    async def generate_append_file_name(
            self,
            store_param: str,
    ):
        """
        Utility function for generating the file name of the blob if the append
        option is enabled, based on the current time.

        :param store_param:
        :return:
        """

        return await generate_append_blob(
            container_client=self.container_client,
            manager=self,
            store_param=store_param,
            time_based=self.timed_append,
        )

    async def process_messages(
            self,
            messages,
            store_param: str,
    ):
        """
        Main method used for storing the messages -- either each event individually,
        or to a single append blob, depending on how the manager was configured.

        :param messages: List of messages or a single message.
        :param store_param: Name of the main directory in the container to store the messages to.
        :return:
        """

        # Since the Event Hub version supports receiving an entire list of events, we automatically turn
        # the received message(s) to a list if it isn't already to minimize code re-usage.
        if not isinstance(messages, list):
            messages = [messages]

        # Data to be stored.
        data = []

        # Get the pre-determined name of the blob to save to, if the append option is enabled.
        # If not, setting it to "None" will generate a random blob name when storing.
        file_name = await self.generate_append_file_name(store_param=store_param) if self.append else None

        for msg in messages:
            data.append(
                form_blob_json_string(
                    msg=msg,
                    get_metadata=self.get_metadata,
                    form_func=self.form_func,
                )
            )

        _, result = await save_to_storage(
            data=data,
            container_client=self.container_client,
            store_param=store_param,
            append=self.append,
            file_append_name=file_name,
        )

        # In the case of non-Hub binding functions, an unhandled exception in the middle of execution will
        # cause the delivery to fail, moving the message to a Dead Letter Queue/triggering a retry attempt.
        # For Event Hub, events will never be re-sent, regardless of the function's success.
        if not result:
            raise PersistorStoreException(
                "FAILED TO SAVE MESSAGE(S) TO STORAGE!"
            )

    async def process_messages_output_blob_binding(
            self,
            messages,
            output_blob: Optional[Out],
    ):
        """
        Store messages/events with a given output blob!

        :param messages: Messages or events to be stored.
        :param output_blob: The output blob to store the message to.
        :return:
        """

        if not isinstance(messages, list):
            messages = [messages]

        data = []

        for msg in messages:

            # Get a (JSON) dictionary containing a "DATA" and "PAYLOAD" fields.
            # Unlike the "standard" processing function, where the payload/metadata extraction
            # is handled in the storage function itself, here, since we're explicitly saving to
            # an output blob, it's much cleaner to just call the data extraction here
            # and then simply set it to the output blob.
            data.append(
                form_blob_json_string(
                    msg=msg,
                    get_metadata=self.get_metadata,
                    form_func=self.form_func,
                )
            )

        # For good measure, we should ensure that the output blob is actually given, lest this function
        # be accidentally wrongly called.
        if output_blob:
            output_blob.set("\n".join(data))
        else:
            raise PersistorStoreException(
                f"NO BLOB BINDING OBJECT GIVEN! FAILED TO STORE: {str(data)}",
            )

    async def run_persistor(
            self,
            req,
            **kwargs,
    ):
        """
        Main method.

        :param req: The single event or list of events the Azure Function was bound to.
        :return:
        """

        # Pass the received event/message to the function to properly process and store
        # the message.
        await self.process_func(
            req,
            **kwargs,
        )
