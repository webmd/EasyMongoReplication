#
# Author:: Matthew Lee (<mlee@webmd.net>)
# Copyright:: Copyright (c) 2015 WebMD, LLC
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging 

class Logger(object):
    def __init__(self, name):
        name = name.replace('.log','')
        logger = logging.getLogger('log_namespace.%s' % name)    # log_namespace can be replaced with your namespace 
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            file_name = os.path.join(os.path.dirname(__file__), '%s.log' % name)    # usually I keep the LOGGING_DIR defined in some global settings file
            handler = logging.FileHandler(file_name)
            formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s %(message)s')
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        self._logger = logger

    def get(self):
        return self._logger


class ReplErrorLogger(object):

    def __init__(self):
        self.logger = Logger(self.__class__.__name__).get()   # accessing the "private" variables for each class

    def do_something(self):
        self.logger.info('Hello')

    def raise_error(self, e):
        self.logger.error(e)



