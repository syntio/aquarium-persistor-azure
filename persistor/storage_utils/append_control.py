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
Used to generate append blobs based on time.
"""

import uuid
from datetime import datetime

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import ContainerClient

from ..service_modules.services_manager_base import ServiceManagerBase
from ..storage_utils.utils_storage import generate_file_name


async def generate_append_blob(
        container_client: ContainerClient,
        manager: ServiceManagerBase,
        store_param: str,
        time_based=False,
):
    """
    Generates an append blob, either based on a random UUID or the current time (hour and minute).

    :param container_client: Container Client.
    :param manager: The manager calling for the new file name generation.
    :param store_param: Name of the folder the persistor will store the data to.
    :param time_based: Whether or not the blob's name should be generated based on the current time.
    :return: Complete file path to store the blob to.
    """

    now = datetime.now()

    # If time-based, set the file name based on time (hour/minute).
    if time_based:
        name = "{hour}-{minute}".format(
            hour=str(now.hour),
            minute=str(now.minute)
        )
    # Otherwise, generate the name based on a random UUID
    else:
        name = str(uuid.uuid4())

    file_name = generate_file_name(store_param, name)

    # To help minimize concurrency issues, we introduce an async lock within the same instance.
    # (Not much we can do with concurrent instances/workers.)
    async with manager.append_lock:

        if not time_based or not manager.now or now.hour > manager.now.hour or now.minute > manager.now.minute:

            blob_client = container_client.get_blob_client(file_name)

            # Try to get the blob properties. If the blob doesn't exist, this will return an exception.
            try:
                await blob_client.get_blob_properties()
            except ResourceNotFoundError:

                # Since the blob doesn't appear to exist, try to create a new one.
                # Since we need to think about concurrency between different instances that
                # don't share the manager object,
                # we have to assume some exception will occur when multiple creation attempts are made.
                try:
                    await blob_client.create_append_blob()
                # Unsure which exception might be thrown if concurrently attempting to create the append blob.
                except:
                    pass

            manager.now = now
            manager.file_name = file_name

    return file_name
