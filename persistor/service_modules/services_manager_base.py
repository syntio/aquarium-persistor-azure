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
Base class for all of the service managers.
"""

import asyncio
from abc import abstractmethod
from typing import Dict

from azure.storage.blob.aio import BlobServiceClient


class ServiceManagerBase:
    """
    Base class used for building the individual service managers.

    :param config: Dictionary containing the necessary configuration parameters (existence of values is not checked).

    """

    def __init__(
            self,
            config: Dict,
    ):

        # If output binding or storage queue mode is enabled, there is no point in even instantiating a blob client on
        # the endpoint.
        self.output_binding = config.get("OUTPUT_BINDING", False)

        if not self.output_binding:
            blob_service_client = BlobServiceClient.from_connection_string(
                conn_str=config["STORAGE_CONNECTION_STRING"]
            )
            self.container_client = blob_service_client.get_container_client(config["CONTAINER_NAME"])
        else:
            self.container_client = None

        self.get_metadata = config.get("GET_METADATA", False)
        self.append = config.get("APPEND", False)
        self.timed_append = config.get("TIMED_APPEND", False)

        # Just in case the user forgot to configure the APPEND config variable.
        if self.timed_append:
            self.append = True

        self.file_name = None

        if self.append:
            self.append_lock = asyncio.Lock()
            self.now = None

    @abstractmethod
    async def run_persistor(
            self,
            req,
            **kwargs,
    ):
        """
        Core method for running the Persistor.

        :param req: The request that triggered the persistor being run (event or HTTP call)
        :return:
        """
