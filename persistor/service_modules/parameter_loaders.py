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
Module containing all of the functions dedicated to loading Function App Configuration settings
through environment variables, packaging them into a configuration dictionary, and using
that dictionary to initialize the proper version of the Persistor.
"""

import os
from typing import Dict, Optional

from ..custom_exceptions.persistor_exceptions import (
    PersistorConfigurationException,
    GeneralBindingConfigurationException,
    EventHubBindingConfigurationException,
    ServiceBusBindingConfigurationException,
    EventHubPullConfigurationException,
    ServiceBusPullConfigurationException,
)
from ..service_modules.services_all_push import PersistorPushManager
from ..service_modules.services_eventhub_pull import PersistorEventHubPullManager
from ..service_modules.services_servicebus_pull import PersistorServiceBusPullManager


def check_params_event_grid(
        store_param: Optional[str],
):
    """
    Does nothing, but there for the sake of a consistent interface.

    :param store_param: The directory in the container the Persistor will store data to
    :return: The same store_param that was passed to the function
    """

    return store_param


def check_params_event_hub_push(
        store_param: Optional[str],
):
    """
    Loads and checks the App Configuration settings for the Event Hub Binding variant.
    Also sets the directory in the container to store the messages to to the topic name, if not set already.

    :param store_param: The directory in the container the Persistor will store data to.
    :return: The (potentially) updated directory name the events will be stored to.
    """

    hub_name = os.getenv("EVENT_HUB_NAME_BINDING", "")
    hub_conn_str = os.getenv("EVENT_HUB_BINDING_CONNECTION_STR", "")
    consumer_group = os.getenv("CONSUMER_GROUP", "$Default")

    if not hub_name:
        raise EventHubBindingConfigurationException("NO EVENT HUB NAME SET!")
    if not hub_conn_str:
        raise EventHubBindingConfigurationException("NO EVENT HUB CONNECTION STRING AVAILABLE!")
    if not consumer_group:
        raise EventHubBindingConfigurationException("NO VALID CONSUMER GROUP NAME GIVEN!")

    if not store_param:
        store_param = hub_name

    return store_param


def check_params_service_bus_push(
        store_param: Optional[str],
):
    """
    Loads and checks the App Configuration settings for the Service Bus Binding variant.
    Also sets the directory in the container to store the messages to, to the topic name, if not set already.

    :param store_param: The directory in the container the Persistor will store data to.
    :return:
    """

    queue_name = os.getenv("SERVICE_BUS_BINDING_QUEUE_NAME", "")
    topic_name = os.getenv("SERVICE_BUS_BINDING_TOPIC_NAME", "")
    sub_name = os.getenv("SERVICE_BUS_BINDING_SUB_NAME", "")
    sb_conn_str = os.getenv("SERVICE_BUS_BINDING_CONNECTION_STRING", "")

    if not queue_name and (not topic_name or not sub_name):
        raise ServiceBusBindingConfigurationException("NO SERVICE BUS QUEUE OR TOPIC/SUBSCRIPTION NAMES GIVEN!")
    if not sb_conn_str:
        raise ServiceBusBindingConfigurationException("NO SERVICE BUS CONNECTION STRING SET!")

    if not store_param:
        store_param = queue_name if queue_name else topic_name

    return store_param


def load_parameters_all_push(
        config: Dict,
):
    """
    Loads the parmeters for all possible binding variations of the Persistor


    :param config: The dictionary of previously loaded App Configuration variables.
    :return: Created Push Manager manager and the dictionary used to configure it.
    """

    binding_services = [
        "EVENT_GRID",
        "EVENT_HUB",
        "SERVICE_BUS",
    ]

    param_checkers_bindings = {
        "EVENT_GRID": check_params_event_grid,
        "EVENT_HUB": check_params_event_hub_push,
        "SERVICE_BUS": check_params_service_bus_push,
    }

    service = os.getenv("BINDING_SERVICE", "").upper()
    store_param = os.getenv("STORE_PARAM", "").upper()

    if not service:
        raise GeneralBindingConfigurationException("NO TYPE OF SERVICE GIVEN FOR THE BINDING!")
    if service not in binding_services:
        raise GeneralBindingConfigurationException(
            "INVALID SERVICE! MUST BE {binding_services}!".format(
                binding_services=", ".join(binding_services)
            )
        )

    store_param = param_checkers_bindings[service](store_param)

    config_to_add = {
        "SERVICE": service,
        "STORE_PARAM": store_param,
    }

    config.update(config_to_add)

    manager = PersistorPushManager(config)

    return manager, config


def load_parameters_event_hub_pull(
        config: Dict,
):
    """
    Loads the parameters for the Event Hub (PULL variant).


    :param config: The dictionary of previously loaded App Configuration variables.
    :return: Created Event Hub Pull manager and the dictionary used to configure it.
    """

    event_hub_conn_str = os.getenv("EVENT_HUB_CONN_STRING", "")
    event_hub_name = os.getenv("EVENT_HUB_NAME", "")
    event_hub_consumer_group = os.getenv("EVENT_HUB_CONSUMER_GROUP", "$Default")
    event_hub_idle_timeout = os.getenv("EVENT_HUB_IDLE_TIMEOUT", None)

    if event_hub_idle_timeout:
        event_hub_idle_timeout = int(event_hub_idle_timeout) if int(event_hub_idle_timeout) >= 10 else 10

    event_hub_receive_duration = os.getenv("EVENT_HUB_RECEIVE_DURATION", "")
    event_hub_prefetch = int(os.getenv("EVENT_HUB_PREFETCH", "512"))
    event_hub_pull_max_batch = int(os.getenv("EVENT_HUB_PULL_MAX_BATCH", "128"))

    if event_hub_receive_duration:
        event_hub_receive_duration = float(event_hub_receive_duration)

    event_hub_checkpoint_storage_conn_str = os.getenv("EVENT_HUB_CHECKPOINT_STORAGE_CONN_STRING", "")
    event_hub_checkpoint_container = os.getenv("EVENT_HUB_CHECKPOINT_CONTAINER", "")
    event_hub_checkpoint_update_rate = os.getenv("EVENT_HUB_CHECKPOINT_UPDATE_RATE", "200")

    event_hub_checkpoint_update_rate = int(event_hub_checkpoint_update_rate) \
        if int(event_hub_checkpoint_update_rate) >= 1 else 1

    if not event_hub_conn_str:
        raise EventHubPullConfigurationException("NO CONNECTION STRING TO EVENT HUB NAMESAPCE GIVEN!")
    if not event_hub_name:
        raise EventHubPullConfigurationException("NO EVENT HUB NAME GIVEN!")
    if not event_hub_checkpoint_container:
        raise EventHubPullConfigurationException("NO CONTAINER GIVEN FOR STORAGE CHECKPOINTING!")

    config_new = {
        "EVENT_HUB_CONNECTION_STRING": event_hub_conn_str,
        "EVENT_HUB_NAME": event_hub_name,
        "EVENT_HUB_CONSUMER_GROUP": event_hub_consumer_group,
        "EVENT_HUB_IDLE_TIMEOUT": event_hub_idle_timeout,
        "EVENT_HUB_RECEIVE_DURATION": event_hub_receive_duration,
        "EVENT_HUB_PREFETCH": event_hub_prefetch,
        "EVENT_HUB_PULL_MAX_BATCH": event_hub_pull_max_batch,
        "EVENT_HUB_CHECKPOINT": event_hub_checkpoint_container,
        "EVENT_HUB_CHECKPOINT_STORAGE_CONN_STR": event_hub_checkpoint_storage_conn_str,
        "EVENT_HUB_CHECKPOINT_UPDATE_RATE": event_hub_checkpoint_update_rate,
    }

    if not event_hub_checkpoint_storage_conn_str:
        config_new["EVENT_HUB_CHECKPOINT_STORAGE_CONN_STR"] = config["STORAGE_CONNECTION_STRING"]

    config.update(config_new)

    manager = PersistorEventHubPullManager(config)

    return manager, config


def load_parameters_service_bus_pull(
        config: Dict,
):
    """
    Loads the parameters for the Service Bus (PULL variant).

    :param config: The dictionary of previously loaded App Configuration variables.
    :return: Created Service Bus Pull manager and the dictionary used to configure it.
    """

    service_bus_conn_str = os.getenv("SERVICE_BUS_CONNECTION_STRING", "")
    service_bus_idle_timeout = os.getenv("SERVICE_BUS_IDLE_TIMEOUT", "0")
    service_bus_prefetch = os.getenv("SERVICE_BUS_PREFETCH", "512")

    if service_bus_idle_timeout:
        # The minimum amount of seconds for idle_timeout is 3 seconds.
        service_bus_idle_timeout = int(service_bus_idle_timeout) \
            if int(service_bus_idle_timeout) >= 3 or int(service_bus_idle_timeout) == 0 else 3

    service_bus_prefetch = int(service_bus_prefetch) if int(service_bus_prefetch) >= 0 else 0

    service_bus_receive_duration = os.getenv("SERVICE_BUS_RECEIVE_DURATION", "")

    if service_bus_receive_duration:
        service_bus_receive_duration = float(service_bus_receive_duration)

    service_bus_type = os.getenv("SERVICE_BUS_TYPE", "").upper()
    service_bus_queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME", "")
    service_bus_topic_name = os.getenv("SERVICE_BUS_TOPIC_NAME", "")
    service_bus_sub_name = os.getenv("SERVICE_BUS_SUB_NAME", "")

    if not service_bus_conn_str:
        raise ServiceBusPullConfigurationException("NO SERVICE BUS CONNECTION STRING GIVEN!")
    if not service_bus_type:
        raise ServiceBusPullConfigurationException("NO SERVICE BUS TYPE GIVEN! (MUST SPECIFY WHETHER QUEUE OR TOPIC!)")
    if service_bus_type not in ["QUEUE", "TOPIC"]:
        raise ServiceBusPullConfigurationException("INVALID SERVICE BUS TYPE! (MUST BE QUEUE OR TOPIC!)")
    if service_bus_type == "QUEUE" and not service_bus_queue_name:
        raise ServiceBusPullConfigurationException("NO SERVICE BUS QUEUE NAME GIVEN!")
    if service_bus_type == "TOPIC" and (not service_bus_topic_name or not service_bus_sub_name):
        raise ServiceBusPullConfigurationException("NO SERVICE BUS TOPIC OR SUBSCRIPTION NAME GIVEN!")

    config_new = {
        "SERVICE_BUS_CONNECTION_STRING": service_bus_conn_str,
        "SERVICE_BUS_TYPE": service_bus_type,
        "SERVICE_BUS_QUEUE_NAME": service_bus_queue_name,
        "SERVICE_BUS_TOPIC_NAME": service_bus_topic_name,
        "SERVICE_BUS_SUB_NAME": service_bus_sub_name,
        "SERVICE_BUS_IDLE_TIMEOUT": service_bus_idle_timeout,
        "SERVICE_BUS_RECEIVE_DURATION": service_bus_receive_duration,
        "SERVICE_BUS_PREFETCH": service_bus_prefetch,
    }

    config.update(config_new)

    manager = PersistorServiceBusPullManager(config)

    return manager, config


ALLOWED_SERVICES = [
    "BINDING",
    "SERVICE_BUS_PULL",
    "EVENT_HUB_PULL",
]

SERVICE_INITS = {
    "BINDING": load_parameters_all_push,
    "EVENT_HUB_PULL": load_parameters_event_hub_pull,
    "SERVICE_BUS_PULL": load_parameters_service_bus_pull,
}


def load_parameters(

):
    """
    Loads the App Configuration variables as environment variables and, depending on them,
    calls a specific function to initialize the Persistor Manager according to the set type.
    Also initializes the Container Client.

    :return: The Persistor Manager and the configuration dictionary that it was configured by.
    """

    app_service = os.getenv("SERVICE_TYPE", "").upper()

    if not app_service:
        raise PersistorConfigurationException(
            "NO SERVICE SPECIFIED! (MUST BE ONE AMONG {service_list}!".format(
                service_list=", ".join(ALLOWED_SERVICES),
            )
        )
    if app_service not in ALLOWED_SERVICES:
        raise PersistorConfigurationException(
            "INVALID SERVICE SPECIFIED! (MUST BE ONE AMONG {service_list}!".format(
                service_list=", ".join(ALLOWED_SERVICES),
            )
        )

    storage_conn_str = os.getenv("PERSISTOR_STORAGE_CONNECTION_STRING", "")
    container = os.getenv("PERSISTOR_CONTAINER_NAME", "")
    append = os.getenv("APPEND", "").upper() == "TRUE"
    timed_append = os.getenv("TIMED_APPEND", "").upper() == "TRUE"
    metadata = os.getenv("GET_METADATA", "").upper() == "TRUE"
    output_binding = os.getenv("PERSISTOR_OUTPUT_BINDING", "").upper() == "TRUE"

    if not storage_conn_str:
        raise PersistorConfigurationException("NO CONNECTION STRING TO AZURE STORAGE GIVEN!")
    if not container:
        raise PersistorConfigurationException("NO CONTAINER NAME FOR PERSISTOR GIVEN!")

    config = {
        "STORAGE_CONNECTION_STRING": storage_conn_str,
        "CONTAINER_NAME": container,
        "APPEND": append,
        "TIMED_APPEND": timed_append,
        "GET_METADATA": metadata,
        "OUTPUT_BINDING": output_binding,
    }

    manager, config = SERVICE_INITS[app_service](config)

    return manager, config
