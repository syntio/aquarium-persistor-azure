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
Contains all of the custom exceptions raised by the Persistor.
"""


class PersistorConfigurationException(Exception):
    """
    Raised when an App Configuration setting is not present or has an invalid value.
    """


class GeneralBindingConfigurationException(PersistorConfigurationException):
    """
    Raised when an App Configuration setting that is shared across all of the Binding/Push
    variants of the Persistor is not present or has an invalid value.
    """


class EventHubBindingConfigurationException(PersistorConfigurationException):
    """
    Raised when an App Configuration setting necessary for an Event Hub Binding variant
    of the Persistor is not present or has an invalid value.
    """


class EventGridBindingConfigurationException(PersistorConfigurationException):
    """
    Raised when an App Configuration setting necessary for an Event Grid Binding variant
    of the Persistor is not present or has an invalid value.
    """


class ServiceBusBindingConfigurationException(PersistorConfigurationException):
    """
    Raised when an App Configuration setting necessary for a Service Bus Binding variant
    of the Persistor is not present or has an invalid value.
    """


class PeristorHTTPTriggerConfigurationException(PersistorConfigurationException):
    """
    Generic exception raised for all HTTP-triggered variations of the Persistor.
    """


class EventHubPullConfigurationException(PeristorHTTPTriggerConfigurationException):
    """
    Raised when an App Configuration setting necessary for an Event Hub Pull variant
    of the Persistor is not present or has an invalid value.
    """


class ServiceBusPullConfigurationException(PeristorHTTPTriggerConfigurationException):
    """
    Raised when an App Configuration setting necessary for a Service Bus Pull variant
    of the Persistor is not present or has an invalid value.
    """


class StorageTypeConfigurationException(Exception):
    """
    Raised when the "APPEND" Configuration setting is set and the passed
    Append Blob Name is either empty or invalid.
    """


class PersistorStoreException(Exception):
    """
    Raised for Binding variant; for Service Bus and Event Grid, raising this exception will
    trigger appropriate mechanisms to mark the delivery as unsuccessful. Event Hub bindings will
    never redo a delivery attempt, no matter the function result.
    """


class InvokerException(Exception):
    """
    Raised when the Invoker Azure Function does not have a link to a function
    to invoke.
    """
