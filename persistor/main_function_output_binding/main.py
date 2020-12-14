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
Entry point for the Azure Function using the output binding.
"""

import azure.functions as func

from ..service_modules.parameter_loaders import load_parameters

# Load the Persistor manager for the Persistor type set in App Configuration.
manager, _ = load_parameters()


async def main(
        req,
        outputb: func.Out[str],
):
    """
    The main function to run.
    Takes the request (which could either be a message/event in case of a binding setup,
    or an HTTP request in case of a Pull function.)

    :param req: The request triggering the function.
    :param outputb: Output binding for the block blob.
    :return:
    """

    await manager.run_persistor(
        req=req,
        output_blob=outputb
    )

    if manager.returnable:
        return "OK"
