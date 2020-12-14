# Azure Persistor

## Overview

The goal of the Persistor project is quite simple: achieving an easily-attachable component to existing pipelines that use Azure messaging services, and store messages to blob storage. The need for such a feature has become a common occurence during development. Trivail mistakes and miscommunication can break even the most well-though-out workflows, leading to major problems; especially if the messages that were supposed to be processed by said workflows failed to be and -- more critically -- cannot be re-sent.

Azure offers three messaging services: Event Grid, Event Hub and Service Bus. Of those three, Event Hub does offer message retention in the form of the Capture functionality. However, it comes with a few caveats. The first is that it's charged by the hour, regardless of the actual number of events being passed through. For high-activity services, this arrangement is, perhaps, more than welcome. However, there are undoubtedly cases where it may look to be an unnecessary extra charge. If the throughput is not high, why not simply be charged for the time it would take to store the messages? In addition, the captured events are all stored in the Avro format, which may or may not be the preferred option to work with, depending on the circumstances.

That's where the idea for the Persistor ultimately comes from. Using Azure's serverless Azure Functions, you can store messages when you need and as cleanly as possible: JSON text files containing the payload and any optional metadata that could've been passed, depending on the used messaging service.

## Features
The Persistor is designed as a single, highly-parameterized Python **Azure Function**, which behaves in different ways, depending on the Configuration of your Function App, depending on your needs and approach.

The Persistor reads the payload of a given event/message (it reads it as a string), and stores it in the following format (a JSON dumped as a string):

```
{"DATA": "PAYLOAD STRING"}
```

In case the Persistor is configured to store more than one event/message into a blob, each of them separated by two newline characters.

Generally, the folder structure of the Persistor is defined as follows:

```
{CONTAINER NAME}
├───{MAIN DESTINATION FOLDER FOR THE SERVICE}
│   ├───{YEAR}
│   │   ├───{MONTH}
│   │   │   ├───{DATE}
│   │   │   │   ├───{BLOB CONTAINING A SINGLE OR SEVERAL EVENTS/MESSAGE}
```

The current "modes" of operation for the Persistor are split into two core modes -- the **Binding** (or, as we internally call it, the **Push** variant), and the **Pull** variant. A full overview is given below:

| Azure Service | Core Approach  | Description                                                                                                                                                                                                                                                                                                                                                                                                                           |
|---------------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Event Grid    | Binding           | Storing through Azure Functions set to an **Event Grid Event binding**.                                                                                                                                                                                                                                                                                                                                                               |
| Event Hub     | Binding           | We call it "binding" because it's an Azure Function set to an **Event Hub binding**; under the hood it is very likely doing some kind of a Pull, but it's triggered automatically and on incoming events, allowing for streaming.                                                                                                                                                                                                        |
| Event Hub     | Pull           | 1 Function, Starting several Parallel Tasks (an **HTTP-triggered** function that pulls messages by concurrently running multiple event hub clients and pulling.                                                                                                                                                                                                                                                                       |
| Event Hub     | Pull + Invoker | 1 Invoker Function, calling several parallel Azure Functions (an **HTTP-triggered** function that calls several whole AZURE FUNCTIONS in parallel; the reason being to take advantage of each function instance getting its own resources at its disposal. Under the hood, it's one function shooting out several HTTP requests to the previously-described "parallel tasks" implementation, and setting their number of tasks to 1.) |
| Service Bus   | Binding           | An Azure Function with a **Service Bus Queue/Topic binding**; in reality you probably won't use the persistor for Queues since, but the option is there if you want to put it at the end of your pipeline to store it to storage.                                                                                                                                                                                                     |
| Service Bus   | Pull           | Much like the Event Hub Pull implementation, uses an **HTTP-triggered** function starting several parallel tasks.                                                                                                                                                                                                                                                                                                                     |
| Service Bus   | Pull + Invoker | 1 Invoker Function, calling several parallel Azure Functions (an **HTTP-triggered** function that calls several whole AZURE FUNCTIONS in parallel; the reason being to take advantage of each function instance getting its own resources at its disposal. Under the hood, it's one function shooting out several HTTP requests to the previously-described "parallel tasks" implementation, and setting their number of tasks to 1.) |

Some additional information:
* Any and all concurrency was achieved through **asyncio**.
* PUSH variants can be configured to either be structured as functions with output binding to a storage blob or call a storage client directly (latter should be used when storing messages in batches.)
* Supports storing in Append Blobs.
* Metadata can be stored along with the event/message payload. (What is considered metadata depends on the service used.)

For a comprehensive and extended explanation of all these features, please consult the [relevant Wiki page](../../wiki/Features).

## Function App Generator

Of course, remembering all these variants and the App Configurations that go along with them would be beyond tedious, as well as difficult to maintain on its own with major updates. That is why the Persistor comes with a **generator**, which creates a deployment-ready folder containing all of the necessary files and dependencies -- allowing you to create as many Persistors as you need, along with all of the mandatory App Setting names listed in the local.settings.json file. The generator is envisioned to be run **locally** (but is, fundamentally, a simple Python script that should, in theory, easily integrated with existing pipelines).

Whichever variant of the Persistor you choose, the generated Function App is meant to be deployed as its own, separate Function App. However, it is entirely possible to configure the generator to deploy the functions in an already existing Function App folder structure, without compromising the existing files, such as local.settings, host or proxy JSONs.

Of course, the local.settings file is generally not deployed along with the rest of the App. However, many pipelines or IDEs do come with options of using it to configure an App. If nothing else, it is a handy way of keeping track of the settings that must be set for the Persistor to function.

## Notes and Limitations
For more information on when you should use the Persistor, its general performance and limitations, please visit the [relevant page on the Wiki.](../../wiki/When-to-Use-the-Persistor%3F)
  
## Repository Structure

The repository is structured as follows:

```
│   .funcignore
│   .gitignore
│   .pylintrc
│   README.md
│   requirements.txt
│   
├───function_generator
│       __init__.py
│       generator.py
│       generator_exceptions.py
│       generator_config.json
│       generator_constants.json
│       local_settings_common.json
│       README.md
│       
├───json_presets
│       event_grid_binding.json
│       event_hub_binding.json
│       host.json
│       http_trigger.json
│       proxies.json
│       service_bus_binding.json
│       
├───persistor
│   ├───custom_exceptions
│   │       __init__.py
│   │       persistor_exceptions.py
│   │       
│   ├───htpp_invoker
│   │       __init__.py
│   │       invoker.py
│   │       
│   ├───main_function
│   │       __init__.py
│   │       main.py
│   │       
│   ├───main_function_output_binding
│   │       __init__.py
│   │       main.py
│   │       
│   ├───service_modules
│   │       __init__.py
│   │       parameter_loaders.py
│   │       services_all_push.py
│   │       services_eventhub_pull.py
│   │       services_http_workers.py
│   │       services_manager_base.py
│   │       services_servicebus_pull.py
│   │       services_utils.py
│   │       
│   └───storage_utils
│           __init__.py
│           append_control.py
│           utils_storage.py
```

The main modules of the Persistor can be found in the **persistor** folder. 

| Folder         | Description                                                                                                               |
|----------------|---------------------------------------------------------------------------------------------------------------------------|
| main_function   | The central function to be ran, regardless of which version of Persistor is to be used, assuming output_binding is turned off.                                   |
| main_function_output_binding   | The central function to be ran, if using a BINDING variant that has an output binding to a blob set.                                   |
| http_invoker    | An extra function that will (optionally) be deployed, if the user decides to use the Pull variants, which use an Invoker. |
| service_modules | The core of the Persistor -- here are the scripts that will be run depending on the chosen use-case.                      |
| storage_utils   | Utilities for saving events/messages to Blob Storage.                                                            |
| custom_exceptions   | Contains the Persistor's custom exceptions.                                                            |

The **json_presets** folder contains generic .json files that will be used in the construction of the Function App folder, such as function.json, host.json and proxies.json.

The **functionGenerator** folder contains the generator that is used to create deployment-ready FunctionApps with a few simple steps. For a more in-depth explanation on the generator, please consult its own respective README, located in its directory.

For general-use, the most important two files are **generator.py** and **generator_config.json**.

## Usage

### Prerequisites
* An Azure Storage account
* An Azure Storage Container the Persistor will save the data to
* An existing messaging service (Event Grid/Event Hub/Service Bus)

### The generator
For a quickstart on how to use the Function Generator, please consult the short overview on the [relevant Wiki page](../../wiki/Generating-the-Azure-Functions).

There is also a [more detailed page](../../wiki/Further-Information-on-the-Function-Generator), containing a more in-depth description on the generator's configuration options.

## Deployment of the generated Function Apps
For a detailed description on how to deploy the generated Azure functions, please visit the [relevant Wiki page](../../wiki/Deploying-the-Azure-Functions).

This page also contains information relevant specifically to each of the Persistor variants. They are all written to be self-contained, so if you only wish to use a single variant, simply reading the instructions for your desired variant should be enough.

## Working with Updates
Whenever the Persistor is updated, you should still be able to simply run the generator again, with the same settings as before, and have your Function Apps updated, without overwriting the existing .json files or deleting any of the additional Azure Functions you might've added as part of the folder. Note that, of course, the actual core Python scripts of the Persistor will be overwritten.

## Links
Issue tracker: https://github.com/syntio/aquarium-persistor-azure/issues
* *In case of sensitive bugs like security vulnerabilities, please contact [support@syntio.net]() directly instead of using issue tracker. We value your effort to improve the security and privacy of this project!*

Wiki: https://github.com/syntio/aquarium-persistor-azure/wiki

## Developed by
The repository is developed and maintained by Syntio Labs.
