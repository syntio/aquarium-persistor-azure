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
Contains all of the utility functions required for saving Blobs (Block or Append) to Azure Storage.
"""

import asyncio
import datetime
import json
import logging
import uuid
from typing import Callable, Generator, Dict, List, Union, Optional

from azure.eventhub import EventData
from azure.functions import (
    EventGridEvent,
    EventHubEvent,
    ServiceBusMessage,
)
from azure.servicebus import Message
from azure.storage.blob.aio import ContainerClient


from ..custom_exceptions.persistor_exceptions import StorageTypeConfigurationException

STORE_RETRY_POLICY_MAX = 3
STORE_RETRY_BACKOFF_TIME = 0.5


def form_data_af_event_grid(
        event: EventGridEvent,
        *args,
):
    """
    Used to extract the payload from an Event Grid Event.

    :param event: An Event Grid Event.
    :return: A JSON-formatted dictionary; its "DATA" field contains the event payload.
    """

    return {"DATA": event.get_json()}


def form_data_af_event_hub_push(
        event: EventHubEvent,
        get_metadata=False,
):
    """
    Used to extract the payload from an Event Hub Event. (PUSH variant.)

    :param event: An Event Hub Event.
    :param get_metadata: Flag determining whether or not to extract the metadata from the Event Hub.
    :return: A JSON-formatted dictionary; its "DATA" field contains the event payload.
    """

    payload = event.get_body().decode("utf-8")
    metadata = None

    if get_metadata:
        metadata = event.metadata

    return form_store(payload, metadata)


def form_data_af_service_bus_push(
        msg: ServiceBusMessage,
        get_metadata=False,
):
    """
    Used to extract the message payload from a Service Bus message. (PUSH variant.)

    :param msg: Service Bus message.
    :param get_metadata: Flag determining whether or not to extract the user_properties.
    :return: A JSON-formatted dictionary containing the "DATA" and (if extracted) "METADATA" fields.
    """

    payload = msg.get_body().decode("utf-8")
    metadata = None

    if get_metadata:
        metadata = msg.user_properties

    return form_store(payload, metadata)


def form_data_af_event_hub_pull(
        event: EventData,
        get_metadata=False,
):
    """
    Used to extract the message from an EventData object. (Event Hub PULL variant.)

    :param event: EventData object from which the payload will be extracted from.
    :param get_metadata: Flag determining whether to retrieve metadata from the properties attribute.
    :return: A JSON-formatted dictionary containing the "DATA" and (if extracted) "METADATA" fields.
    """

    payload = event.body_as_str()
    metadata = None

    # For some unknown the official Microsoft documentation does not disclose that, when
    # manually retrieving messages from the Event Hub, it encodes both the keys and values
    # of custom properties into bytes, unlike its binding variation.
    if get_metadata:
        metadata = event.properties
        if metadata:
            metadata = {k.decode("utf-8"): metadata[k].decode("utf-8") for k in metadata}

    return form_store(payload, metadata)


def form_data_af_service_bus_pull(
        msg: Message,
        get_metadata=False,
):
    """
    Used to extract the message payload from a Service Bus message. (PULL variant.)

    :param msg: Service Bus message.
    :param get_metadata: Flag determining whether or not to extract the user_properties.
    :return: A JSON-formatted dictionary containing the "DATA" and (if extracted) "METADATA" fields.
    """

    payload = msg.body
    metadata = None

    if isinstance(payload, Generator):
        message_body = bytearray()
        for payload_bytes in payload:
            message_body.extend(bytes(payload_bytes))
        payload = message_body

    payload = payload.decode("utf-8")

    # For some unknown the official Microsoft documentation does not disclose that, when
    # manually retrieving messages from Service Bus, it encodes both the keys and values of custom
    # properties into bytes, unlike its binding variation.
    if get_metadata:
        metadata = msg.user_properties
        if metadata:
            metadata = {k.decode("utf-8"): metadata[k].decode("utf-8") for k in metadata}

    return form_store(payload, metadata)


def form_store(
        payload: Union[Dict, str],
        metadata: Union[Dict, str, None],
):
    """
    Processes the payload and the metadata (if any exists) into a JSON-like format.

    :param payload: A string object or dictionary containing the message data.
    :param metadata: A dictionary containing the message metadata.
    :return: A dictionary with "DATA" and (optionally) "METADATA" fields.
    """

    data = {"DATA": payload}

    if metadata:
        data["METADATA"] = metadata

    return data


def generate_file_name(
        store_param: str,
        blob_name: Optional[str] = None,
):
    """
    Generates the file name string for a blob.

    :param store_param: The main folder in the container to store the file in.
    :param blob_name: Name of the blob itself.
    :return: Generated file name.
    """

    if not blob_name:
        blob_name = str(uuid.uuid4())

    now = datetime.datetime.now()

    file_name = "{store_param}/{year}/{month}/{day}/{blob_name}.txt".format(
        store_param=store_param,
        year=str(now.year),
        month=str(now.month),
        day=str(now.day),
        blob_name=blob_name,
    )

    return file_name


def form_blob_json_string(
        msg: Union[EventGridEvent, EventHubEvent, ServiceBusMessage, EventData, Message],
        get_metadata: bool,
        form_func: Callable,
):
    """
    Create a blob JSON string from a message and the form function to process it with.

    :param msg: Message/event to store.
    :param get_metadata: Boolean determining whether or not to retrieve metadata from the message (if possible)
    :param form_func: Function to extract the payload from the message/event object.
    :return: JSON-string containing the data and metadata information.
    """

    return json.dumps(form_func(msg, get_metadata))


async def save_to_storage(
        data: List[str],
        container_client: ContainerClient,
        store_param: str,
        append=False,
        file_append_name: Optional[str] = None,
):
    """
    Saves message to storage.

    :param data: List of data be stored.
    :param container_client: Blob service client (initialized outside this function).
    :param store_param: The main folder in the container to store the file in.
    :param append: Flag to determine whether the data should be appended to an append blob.
    :param file_append_name: Name of the append blob to store to. Ignored if append is False.
    :return: Name of the blob stored to and result
    """

    # Success flag.
    result = False

    # Get the blob file name and the data (string) to store.
    if not append:
        file_name = generate_file_name(store_param=store_param)
    else:
        file_name = None

    # If the file_name is None, we should be using the append blob name.
    # If the append blob name is not given, an exception is raised.
    if not file_name:
        if not file_append_name:
            raise StorageTypeConfigurationException("SET BLOB TO 'APPEND', YET NO FILE GIVEN FOR THE APPEND BLOB!")
        file_name = file_append_name

    # Store the data utilizing a simple retry policy.
    # In truth, the Blob Client we're using already uses an ExponentialRetry mechanic. This is
    # merely an additional fail-safe to it, in case the library at some point changes some of
    # the default retry parameters or the save load per second is far bigger than expected.
    # In addition, on the off-chance the user is using the TIMED_APPEND option, this retry loop helps with
    # potential concurrency issues. If the function manages to get to this point without an append blob
    # existing, this loop will give enough time for it to be created in the meantime and ensure a successful
    # store.
    # In practice, this loop will not execute more than once.
    for i in range(STORE_RETRY_POLICY_MAX):

        try:

            # We include getting the blob client in the retry, due to the fact we likely
            # need to renew the lease for the blob.
            blob_client = container_client.get_blob_client(
                file_name,
            )

            if append:
                store_method = blob_client.append_block
            else:
                store_method = blob_client.upload_blob

            async with blob_client:
                await store_method("\n".join(data))

            # Set the result to true.
            result = True

            # Escape the retry loop.
            break
        # Currently set to catch a general exception, seeing as how the documentation doesn't
        # actually state the possible exceptions that could occur during these processes.
        except Exception as exc:

            if i == STORE_RETRY_POLICY_MAX - 1:
                logging.error(
                    "FAILED TO SAVE TO STORAGE! | PATH: %s | FAILED MESSAGE CONTENTS: %s | EXCEPTION %s",
                    file_name,
                    data,
                    str(exc),
                )
            else:
                logging.warning(
                    "FAILED TO SAVE TO STORAGE! | PATH: %s | RETRYING... (ATTEMPT NO. %s)",
                    file_name,
                    str(i + 1),
                )
                await asyncio.sleep(STORE_RETRY_BACKOFF_TIME)

    return file_name, result
