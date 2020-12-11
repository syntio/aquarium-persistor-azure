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
The Generator module.
When run, generates a deployment-ready Function App folder structure for the Peristor
types specified in the generator_config.json file.
"""

import copy
import json
import logging
import os
import pathlib
import re
import sys
import textwrap
import uuid

from distutils.dir_util import copy_tree as copytree
from shutil import copyfile

from generator_exceptions import NoServicesGivenException

JSON_STRING_INDENT = 2
PRINT_LEVEL_INDENT = 2

curr_directory = pathlib.Path(__file__).parent.absolute()
core_directory = pathlib.Path(__file__).parent.parent.absolute()

GENERATOR_CONSTANTS_PATH = f"{curr_directory}/generator_constants.json"
GENERATOR_CONFIG_PATH = f"{curr_directory}/generator_config.json"
GENERATOR_CONFIG_LOCAL_SETTINGS_PATH = f"{curr_directory}/local_settings_common.json"

SERVICE_MODULES_PATH = f"{core_directory}/persistor/service_modules/"
STORAGE_UTILS_PATH = f"{core_directory}/persistor/storage_utils/"
CUSTOM_EXCEPTIONS_PATH = f"{core_directory}/persistor/custom_exceptions"
REQUIREMENTS_PATH = f"{core_directory}/requirements.txt"
FUNCIGNORE_PATH = f"{core_directory}/.funcignore"
HOST_PATH = f"{core_directory}/json_presets/host.json"
PROXIES_PATH = f"{core_directory}/json_presets/proxies.json"
JSON_PRESETS_PATH = f"{core_directory}/json_presets/"
MAIN_FUNC_PATH = f"{core_directory}/persistor/main_function/"
INVOKER_FUNC_PATH = f"{core_directory}/persistor/http_invoker"
INVOKER_FUNC_JSON = f"{JSON_PRESETS_PATH}http_trigger.json"

PRESET_EMPTY_LOCAL_SETTINGS_JSON = {
    "isEncrypted": False,
    "Values": {
        "AzureWebJobsStorage": "",
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "FUNCTIONS_WORKER_PROCESS_COUNT": "10",
    }
}

PRESET_BLOB_BINDING = {
    "type": "blob",
    "direction": "out",
    "name": "outputb",
    "path": "%PERSISTOR_CONTAINER_NAME%/%STORE_PARAM%/{datetime:yyyy}/{datetime:MM}/{datetime:dd}/{rand-guid}.txt",
    "connection": "PERSISTOR_STORAGE_CONNECTION_STRING"
}

# Set up the logger to STDOUT (or wherever is the most appropriate).
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
)


def perform_copy(
        path_from,
        path_to,
        copy_func,
        overwrite,
):
    """
    Copies a file or folder with a given copy function.
    Also allows to set whether or not to overwrite previously existing
    files or folders.

    :param path_from: Source path.
    :param path_to: Destination path.
    :param copy_func: Function used to copy.
    :param overwrite: Whether or not to overwrite if the files/folders already exist.
    :return:
    """

    if overwrite or not os.path.exists(path_to):
        copy_func(path_from, path_to)


def copy_reqirements(
        path_from,
        path_to,
):
    """
    Copies the requirements.txt file.
    If the file already exist on the destination path, alter it to match
    the Persistor's requirements.

    :param path_from: Source path.
    :param path_to: Destination path.
    :return:
    """

    if os.path.exists(path_to):
        with open(path_to, "r") as reqs_to:
            req_lines = reqs_to.readlines()

        if req_lines[-1][-1] != "\n":
            req_lines[-1] = f"{req_lines[-1]}\n"

        req_dict = {re.split("==|~=|>=", line)[0]: line for line in req_lines}

        with open(path_from, "r") as reqs_from:
            req_lines_new = reqs_from.readlines()

        if req_lines_new[-1][-1] != "\n":
            req_lines_new[-1] = f"{req_lines_new[-1]}\n"

        for line in req_lines_new:
            req_dict[re.split("==|~=|>=", line)[0]] = line

        with open(path_to, "w") as reqs_final:
            reqs_final.writelines(req_dict.values())

    else:
        copyfile(path_from, path_to)


def copy_utils(
        app_path,
):
    """
    Copy utility functions and modules used for all of the neessary services to the
    given Function App path.

    :param app_path: Path to the output application folder.
    :return:
    """

    # Create path strings.
    service_modules = f"{app_path}service_modules/"
    storage_utils = f"{app_path}storage_utils/"
    custom_exceptions = f"{app_path}custom_exceptions/"
    requirements = f"{app_path}requirements.txt"
    host = f"{app_path}host.json"
    proxies = f"{app_path}proxies.json"
    funcignore = f"{app_path}.funcignore"

    # A complete list of files and folders to copy, along with information on
    # whether or not to overwrite existing folders/files, in accordance with out
    # philosophy of not disturbing existing Function Apps
    copies = [
        (SERVICE_MODULES_PATH, service_modules, copytree, True),
        (STORAGE_UTILS_PATH, storage_utils, copytree, True),
        (CUSTOM_EXCEPTIONS_PATH, custom_exceptions, copytree, True),
        (FUNCIGNORE_PATH, funcignore, copyfile, False),
        (HOST_PATH, host, copyfile, False),
        (PROXIES_PATH, proxies, copyfile, False),
    ]

    for to_copy in copies:
        perform_copy(to_copy[0], to_copy[1], to_copy[2], to_copy[3])

    # Copy the requirements, changing/updating where necessary, while trying not to touch the already
    # existing requirements.
    copy_reqirements(REQUIREMENTS_PATH, requirements)


def modify_function_json_to_include_binding(
        config,
        extra_config,
        app_type,
        app_path,
):
    """
    Adds the output binding information as needed to the function.json file of the function!

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :param app_path: Path to the output folder for the Function App.
    :return: The local.settings.json dictionary currently being formed.
    """

    if extra_config.get("output_binding", "false").lower() == "true":
        function_json_path = f"{app_path}{app_type}/function.json"

        with open(function_json_path, "r") as f_func_json:
            function_json = json.loads(f_func_json.read())

        function_json["bindings"].append(PRESET_BLOB_BINDING)

        with open(function_json_path, "w") as f_func_json:
            f_func_json.write(json.dumps(function_json, indent=JSON_STRING_INDENT))

        logging.info(textwrap.indent(
            text="CONFIGURED function.json for BLOB OUTPUT BINDING!",
            prefix=' ' * PRINT_LEVEL_INDENT * 8,
        ))

    return config


def ensure_timed_append_if_cardinality_one(
        config,
        extra_config,
        app_type,
        app_path,
):
    """
    Automatically sets the "TIMED_APPEND" App Configuration variable automatically to "TRUE" if
    the "append" option is enabled and the cardinality parameter (if given) is set to "one".

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :param app_path: Path to the output folder for the Function App.
    :return: The local.settings.json dictionary currently being formed.
    """

    if extra_config["append"].lower() == "true" \
            and "cardinality" in extra_config and extra_config["cardinality"].lower() == "one":
        config["Values"]["TIMED_APPEND"] = "TRUE"

    return config


def ensure_append_true_if_timed_append(
        config,
        extra_config,
        app_type,
        app_path,
):
    """
    Sets the "APPEND" App Configuration variable automatically to "TRUE" if the "timed_append" option
    is enabled.

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :param app_path: Path to the output folder for the Function App.
    :return: The local.settings.json dictionary currently being formed.
    """

    if extra_config.get("timed_append", "false").lower() == "true":
        config["Values"]["APPEND"] = "TRUE"

    return config


def edit_sb_type_function_json(
        config,
        extra_config,
        app_type,
        app_path,
):
    """
    If the user configured a specific Service Bus type, edit the function.json of the function
    and the local.settings.json to only feature the options for the selected type, instead of
    forcing the user to manually delete the one they don't want.

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :param app_path: Path to the output folder for the Function App.
    :return: The local.settings.json dictionary currently being formed.
    """

    function_json_path = f"{app_path}{app_type}/function.json"

    with open(function_json_path, "r") as f_func_json:
        function_json = json.loads(f_func_json.read())

    if extra_config["sb_type"].lower() == "topic":
        config["Values"]["SERVICE_BUS_TYPE"] = "TOPIC"
        if "binding" in app_type:
            del config["Values"]["SERVICE_BUS_BINDING_QUEUE_NAME"]
            del function_json["bindings"][0]["queueName"]
        else:
            del config["Values"]["SERVICE_BUS_QUEUE_NAME"]
    else:
        config["Values"]["SERVICE_BUS_TYPE"] = "QUEUE"
        if "binding" in app_type:
            del function_json["bindings"][0]["topicName"]
            del function_json["bindings"][0]["subscriptionName"]
            del config["Values"]["SERVICE_BUS_BINDING_TOPIC_NAME"]
            del config["Values"]["SERVICE_BUS_BINDING_SUB_NAME"]
        else:
            del config["Values"]["SERVICE_BUS_TOPIC_NAME"]
            del config["Values"]["SERVICE_BUS_SUB_NAME"]

    with open(function_json_path, "w") as f_func_json:
        f_func_json.write(json.dumps(function_json, indent=JSON_STRING_INDENT))

    logging.info(textwrap.indent(
        text="CONFIGURED function.json AND local.settings.json FOR CHOSEN SERVICE BUS TYPE!",
        prefix=' ' * PRINT_LEVEL_INDENT * 8,
    ))

    return config


def copy_http_invoker_func_data(
        config,
        extra_config,
        app_type,
        app_path,
):
    """
    If the user configured a PULL function using the Invoker variant, this function generates
    the necessary files to deploy the invocation function in the same function app as the
    main PULL function.

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type (Unnecessary, here only for abstraction purposes).
    :param app_path: Path to the output folder for the Function App.
    :return: The local.settings.json dictionary currently being formed.
    """

    # If the user has chosen to enable the invoker with their function, add the necessary files.
    if extra_config.get("invoker").lower() == "true":
        invoker_path = f"{app_path}http_invoker"
        invoker_func_json_path = f"{invoker_path}/function.json"

        copytree(INVOKER_FUNC_PATH, invoker_path)
        copyfile(
            INVOKER_FUNC_JSON,
            invoker_func_json_path,
        )

        with open(invoker_func_json_path, "r") as f_invoker_func_json:
            inv_func = json.loads(f_invoker_func_json.read())

        inv_func["scriptFile"] = "invoker.py"

        with open(invoker_func_json_path, "w") as f_invoker_func_json:
            f_invoker_func_json.write(json.dumps(inv_func, indent=JSON_STRING_INDENT))

        logging.info(textwrap.indent(
            text="COPIED INVOKER DATA",
            prefix=' ' * PRINT_LEVEL_INDENT * 8,
        ))

    return config


def edit_cardinality_function_json(
        config,
        extra_config,
        app_type,
        app_path,
):
    """
    If the user configured a function that allows to set a cardinality of the received data ('one' or 'many'),
    this function will configure the function.json file and local.settings.json file for the user's
    specific option.

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :param app_path: Path to the output folder for the Function App.
    :return: The local.settings.json dictionary currently being formed.
    """

    function_json_path = f"{app_path}{app_type}/function.json"

    with open(function_json_path, "r") as f_func_json:
        function_json = json.loads(f_func_json.read())

    function_json["bindings"][0]["cardinality"] = extra_config["cardinality"].lower()

    with open(function_json_path, "w") as f_func_json:
        f_func_json.write(json.dumps(function_json, indent=JSON_STRING_INDENT))

    logging.info(textwrap.indent(
        text="CONFIGURED function.json FOR CHOSEN CARDINALITY",
        prefix=' ' * PRINT_LEVEL_INDENT * 8,
    ))

    return config


def check_extra_configs(
        extra_config,
        app_type,
):
    """
    Checks if the additional generator configuration settings are actual generator configuration settings and,
    if they are, if they're in the valid value range.

    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :return: Dictionary configuring all of the valid additional options passed by user in generator_config.json.
    """

    for extra_feature in list(extra_config.keys()):
        if extra_feature.lower() not in ADDITIONAL_FEATURES or \
                (extra_config[extra_feature.lower()] not in ADDITIONAL_FEATURES_ACCEPTABLE_VALUES[extra_feature]
                 and len(ADDITIONAL_FEATURES_ACCEPTABLE_VALUES[extra_feature]) != 0) or \
                app_type not in ADDITIONAL_FEATURES[extra_feature.lower()]:
            del extra_config[extra_feature]
            logging.info(textwrap.indent(
                text=f"INVALID EXTRA SETTING REMOVED: {extra_feature}",
                prefix=' ' * PRINT_LEVEL_INDENT * 6,
            ))

    return extra_config


def special_extra_configs(
        config,
        extra_config,
        app_type,
        app_path,
        settings,
):
    """
    Updates the dictionary that will be exported to local.settings.json by putting in the configuration
    variables necessary for some extra features.
    Also runs any necessary procedures/scripts by editing the files of the Function App to make
    the feature possible to use.

    :param config: Configuration dictionary (the one that gets exported to local.settings.json).
    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param app_type: Application type.
    :param app_path: Path to the output folder for the Function App.
    :param settings: List containing additional app configuration settings that need to be placed for a given Persistor
    type and/or feature.
    :return: The local.settings.json dictionary currently being formed.
    """

    logging.info(textwrap.indent(
        text="IMPORTING ADDITIONAL SETTINGS",
        prefix=' ' * PRINT_LEVEL_INDENT * 6,
    ))

    # Go through all of the extra features set in the generator configuration file.
    # Check if they are an actual configurable feature, check if that flag can be set for he function type,
    # and check if the value of that flag is an acceptable value.
    for extra_feature in extra_config:

        # If the feature generates only a single additional App Setting, set it immediately to what
        # the user had set it.
        if settings[extra_feature.lower()] in ADDITIONAL_FEATURES_SIMPLE_SINGLE_VALUE:
            config["Values"][settings[extra_feature][0]] = extra_config[extra_feature].upper()

        # Otherwise, add all of the necessary additional App Settings, setting them to empty
        else:
            for config_var in settings[extra_feature]:
                config["Values"][config_var] = ""

        # Once the App Settings(s) are set, check if there is anything else to do, either with the files
        # or the configuration dictionary
        if extra_feature.lower() in ADDITIONAL_PROCEDURES:
            config = ADDITIONAL_PROCEDURES[extra_feature](
                config,
                extra_config,
                app_type,
                app_path,
            )

    return config


def special_extra_configs_update_existing_local_settings_json(
        extra_config,
        config,
        settings,
):
    """
    Utility function.
    If an additional generator configuration feature has only one App Setting to set, and the local.settings.json
    file already exists, set it to the new value set by the user, overwriting the previous version.

    :param extra_config: Dictionary configuring the additional options passed by user in generator_config.json.
    :param config: A dictionary containing the current data of local.settings.json.
    :param settings: List containing additional app configuration settings that need to be placed for a given Persistor
    type and/or feature.
    :return: The local.settings.json dictionary currently being formed.
    """

    for extra_feature in extra_config:
        if extra_feature in ADDITIONAL_FEATURES_SIMPLE_SINGLE_VALUE:
            config["Values"][settings[extra_feature][0]] = extra_config[extra_feature].upper()

    return config


def local_settings_create(
        app_type,
        app_path,
        extra_config,
):
    """
    Generates the local.settings.json file from a given dictionary.

    :param app_type: Type of Function App (type of persistor) being generated.
    :param app_path: Path to the output folder for the Function App.
    :param extra_config: Dictionary containing any additional parameters the user set in generator_config.json.
    :return:
    """

    # Open the JSON containing all of the App Settings information.
    with open(GENERATOR_CONFIG_LOCAL_SETTINGS_PATH, "r") as f_generator_settings:
        settings = json.loads(f_generator_settings.read())

    # Create a basic dictionary used in local.settings.json.
    config = copy.deepcopy(PRESET_EMPTY_LOCAL_SETTINGS_JSON)

    # Set the App Settings all versions of the Persistor share.
    for shared in settings["shared"]:
        config["Values"][shared] = ""

    # If the Persistor is of the binding type, set the Persistor settings all
    # binding-type Persistors share.
    if "binding" in app_type:
        for binding_shared in settings["shared_bindings"]:
            config["Values"][binding_shared] = ""
        config["Values"]["BINDING_SERVICE"] = app_type[:-8]
        config["Values"]["SERVICE_TYPE"] = "BINDING"
    else:
        config["Values"]["SERVICE_TYPE"] = app_type.upper()

    # Set the App Settings specific to a certain type of Persistor.
    for specific in settings[app_type]:
        config["Values"][specific] = ""

    logging.info(textwrap.indent(
        text="BASE LOCAL SETTINGS IMPORTED",
        prefix=' ' * PRINT_LEVEL_INDENT * 4,
    ))

    # Check if additional generator parameters are valid.
    extra_config = check_extra_configs(
        extra_config,
        app_type,
    )

    # If additional configuration exists and is valid, set additional App Settings depending
    # on the extra configuration parameters given by the user.
    if extra_config:
        config = special_extra_configs(
            config,
            extra_config,
            app_type,
            app_path,
            settings,
        )

        logging.info(textwrap.indent(
            text="ADDITIONAL LOCAL SETTINGS IMPORTED",
            prefix=' ' * PRINT_LEVEL_INDENT * 4,
        ))

    local_settings_path = f"{app_path}local.settings.json"

    # If the user is re-deploying the app to an already existing folder, make sure not to overwrite
    # the existing local.settings.json; instead, simply add values that are not already present.
    if os.path.exists(local_settings_path):
        with open(local_settings_path, "r") as f_app_settings:
            current_settings = json.loads(f_app_settings.read())

        curr_keys = set(current_settings["Values"].keys())
        new_keys = set(config["Values"].keys())

        diff = list(new_keys - curr_keys)

        for setting in settings["immediately_updatable"]:
            if setting in config["Values"]:
                diff.append(setting)

        current_settings["Values"].update({k: config["Values"][k] for k in diff})

        config = current_settings

    if extra_config:
        config = special_extra_configs_update_existing_local_settings_json(
            extra_config,
            config,
            settings,
        )

    local_settings = json.dumps(config, indent=JSON_STRING_INDENT)

    with open(local_settings_path, "w") as f_app_settings:
        f_app_settings.write(local_settings)


def app_create(
        app_type,
        app_name,
        output_path,
        extra_config,
):
    """
    Creates the Function App folder, containing all the necessary files and configuration samples.

    :param app_type: Type of Function App (type of Persistor) being generated.
    :param app_name: Name of the Function App.
    :param output_path: Path to the output folder.
    :param extra_config: Dictionary containing any additional parameters the user set in generator_config.json.
    :return:
    """

    logging.info("CREATING FILES FOR APP: %s", app_name)

    app_path = f"{output_path}{app_name}/"
    main_func_path = f"{app_path}{app_type}"

    if "output_binding" in extra_config and extra_config["output_binding"].lower() == "true":
        main_func_origin = "{original_path}_output_binding/".format(
            original_path=MAIN_FUNC_PATH[:-1]
        )
    else:
        main_func_origin = MAIN_FUNC_PATH

    # Copy the main function.
    copytree(main_func_origin, main_func_path)

    logging.info(textwrap.indent(
        text="COPIED MAIN FUNCTION",
        prefix=' ' * PRINT_LEVEL_INDENT * 2,
    ))

    # Copy the function.json specific to the chosen Persistor type.
    copyfile(
        f"{JSON_PRESETS_PATH}{PERSISTOR_TYPE_FUNC_TYPE[app_type]}.json",
        f"{main_func_path}/function.json",
    )

    logging.info(textwrap.indent(
        text="COPIED FUNCTION.JSON",
        prefix=' ' * PRINT_LEVEL_INDENT * 2,
    ))

    # Copy all of the necessary utilities.
    copy_utils(app_path)

    logging.info(textwrap.indent(
        text="COPIED ALL UTILITIES",
        prefix=' ' * PRINT_LEVEL_INDENT * 2,
    ))

    logging.info(textwrap.indent(
        text="CONFIGURING local.settings.json",
        prefix=' ' * PRINT_LEVEL_INDENT * 2,
    ))

    # Create a local.settings.json based on the App Type and given configurations.
    local_settings_create(app_type, app_path, extra_config)

    logging.info(textwrap.indent(
        text="COPIED local.settings.json",
        prefix=' ' * PRINT_LEVEL_INDENT * 2,
    ))

    logging.info(textwrap.indent(
        text="APP FOLDER STRUCTURE CREATED!\n\n",
        prefix=' ' * PRINT_LEVEL_INDENT * 2,
    ))


# General module initialization.
with open(GENERATOR_CONSTANTS_PATH, "r") as constants_f:
    constants = json.loads(constants_f.read())

# Load the names of the function.jsons to be used for every type of Persistor.
PERSISTOR_TYPE_FUNC_TYPE = constants["appTypeFunctionJSON"]

# Load a dictionary containing which additional settings the user can set for the generator,
# as well as which specific Persistor types even support the additional setting.
ADDITIONAL_FEATURES = constants["additionalFeaturesAcceptables"]

# Load the acceptable values for the additional generator configuration settings.
ADDITIONAL_FEATURES_ACCEPTABLE_VALUES = constants["additionalFeaturesAllowedValues"]

# Set which additional features have a single value that can directly be passed into local.settings.json.
ADDITIONAL_FEATURES_SIMPLE_SINGLE_VALUE = constants["additionalFeaturesSingleConfigValue"]

# Functions to be triggered for certain additional features (usually involve additional
# file editing.)
ADDITIONAL_PROCEDURES = {
    "invoker": copy_http_invoker_func_data,
    "cardinality": edit_cardinality_function_json,
    "sb_type": edit_sb_type_function_json,
    "append": ensure_timed_append_if_cardinality_one,
    "timed_append": ensure_append_true_if_timed_append,
    "output_binding": modify_function_json_to_include_binding,
}

if __name__ == "__main__":

    with open(GENERATOR_CONFIG_PATH, "r") as f:

        conf = json.loads(f.read())

        if "persistorServices" not in conf or not conf["persistorServices"]:
            raise NoServicesGivenException("NO SERVICES FOR THE PERSISTOR TO CREATE FOR WERE SPECIFIED! CANCELING!")

        OUTPUT_F = "../output/{app_name}/".format(app_name=str(uuid.uuid4())) \
            if ("output" not in conf or not conf["output"]) \
            else "{output_path}/".format(output_path=conf["output"])

    apps_to_make = conf["persistorServices"]

    for app in apps_to_make:

        if len(app) < 3:
            app.append({})

        app_create(
            app[0].lower(),
            app[1],
            OUTPUT_F,
            app[2]
        )
