# The Persistor Function App Generator

## Overview

The "output" key contains the path to the folder in which you will be deploying your Function Apps. Note that, in this case, "test" is not the name of the Function App, but rather the folder in which one or several Function Apps folders will be placed in.

The "output" folder need not exist beforehand. Running the generator multiple times with the same destination folder will overwrite the existing functions.
However, if the generator is run, and some of the functions are removed from the configuration JSON, and the generator is re-run (with the same output folder), the functions removed from the configuration
JSON will NOT be removed from the output folder.

Note that if you're using a relative path when setting the output directory, it will be relative to **the active directory you are running the function generator script from**.

The "persistorServices" key contains a list. Each item of this list is another list, consisting of three times, all laid out in a specific order:
* The first is the **TYPE** of Persistor you've chosen to use. The allowed options are:

``` 
EVENT_GRID_BINDING, EVENT_HUB_BINDING, SERVICE_BUS_BINDING, EVENT_HUB_PULL, SERVICE_BUS_PULL 
```

* The second is the **NAME** of the Function App you wish to create. Note that, depending on how you set the "output" folder, you can set this to a name of an existing Function App folder structure, and make the Persistor into just one function that's a part of it, instead of the whole app.

* The third is an **EXTRA CONFIGURATION** dictionary. This allows to deploy the Persistor with some additional features. For instance, if you use the Persistor variant that uses the invoker function, along with a Service Bus "PULL" variant, you would put a dictionary like so:

```
["SERVICE_BUS_PULL", "serviceBusPullTestWithInvoker", {"invoker":  "true"}]
```

After setting the generator configuration file, simply run the generator. Your function Apps should be ready near-instantaneously.

## Running the Generator
After setting the configuration JSON, the generator can simply be run with:

``` 
python generator.py
``` 

## Supported Configuration
Below is an overview of the additional features that can be set when creating the Persistor folder structure. Almost all of these add another App Configuration variable in the local.settings.json. Some of those will need to be configured after the fact like the other variables, some will be immediately filled with the setting given in the dictionary.

| Variable Name | Relevant Persistor Types                                                               | Possible Values    | App Configuration Settings Added                                                                                                                                                                                                                                             | App Configuration Setting Automatically Set?                                     |
|---------------|----------------------------------------------------------------------------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| invoker       | SERVICE BUS (PULL)<br>EVENT HUB (PULL)                                                 | "true"<br>"false"  | "FUNC_TO_INVOKE"<br>(An HTTP link to the Azure Function that the Invoker will trigger.)<br>"INVOKER_TIMEOUT"<br>(Sets the amount of time, in seconds, the invoker HTTP function will wait for a response.)<br>"DELAY_BETWEEN_FUNCS"<br>(Signifies the delay between each function invocation, in seconds.)                                                                                                                                                                                      | -<br>(If not defined, "INVOKER_TIMEOUT" defaults to 75 seconds, and "DELAY_BETWEEN_FUNCS" defaults to 0.25 seconds.)                                                                                |
| cardinality   | EVENT HUB (BINDING)                                                                    | "one"<br>"many"    | In addition, modifies the "function.json" file adding the exact same parameter, setting it to the chosen setting.                                                                                                                                       | + <br>(Automatically set to what was defined in this field.)                     |
| sb_type       | SERVICE BUS (PULL)<br>SERVICE BUS (BINDING)                                            | "queue"<br>"topic" | If this setting is not set, the generated Azure App Folder structure will include both the queueName and topic/subscriptionName fields in the function app's local.settings.json and/or function.json files. If this setting is given, the redundant fields will be removed. | -<br>(Although the superfluous one is removed, it still need to be manually set.) |
| append        | ALL SERVICES | "true"<br>"false"  | "APPEND"<br>(Determines whether or not to use an append blob.)                                                                                                                                                                                                               | + <br>(Automatically set to what was defined in this field.)                     |
| timed_append        | ALL SERVICES | "true"<br>"false"  | "TIMED_APPEND"<br>(Determines whether or not the append blob should constantly be created based on the current execution time.)                                                                                                                                                                                                               | + <br>(Automatically set to what was defined in this field.)                     |
| metadata      | EVENT HUB (BINDING)<br>EVENT HUB (PULL)<br>SERVICE BUS (BINDING)<br>SERVICE BUS (PULL)                     | "true"<br>"false"  | "GET_METADATA"<br>(Determines whether or not to store messages alongside the message/event payload. In the case of the Service Bus, it is the custom, user-defined metadata, wheras with the Event Hub, the metadata generated at the time of arrival is stored.)            | + <br>(Automatically set to what was defined in this field.)                     |
| output_binding      | ALL BINDING SERVICES                     | "true"<br>"false"  | "PERSISTOR_OUTPUT_BINDING"<br>(Determines whether or not the deployed function is set up to have an output binding to a blob as the store method.)            | + <br>(Automatically set to what was defined in this field.)                     |

If the setting is set but mismatched to its relevant Persistor type, or the value given isn't among the valid ones, the setting will simply be ignored.

## Running the Generator on existing Function App folders
As mentioned earlier, the Generator will not overwrite or delete anything in an existing Folder App structure, or the local.settings.json files. Instead, it will merely add additional App Configuration settings needed by the Persistor. 

If you are updating, it will not overwrite the previously set values in the local.settings.json. (The only exception being the configuration setting that has a + under the "App Configuration Setting Automatically Set?" column in the table above -- those will be overwritten should the user specify another value in the generator_config file for a given function.)



