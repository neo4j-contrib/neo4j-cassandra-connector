#!/usr/bin/env python
# -*- coding: utf-8 -*- 
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)


import logging
import os
import sys


if sys.version_info[0] == 3:
    unicode = str

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
    from unittest2.case import SkipTest
else:
    import unittest
    from unittest.case import SkipTest


logging.basicConfig(stream=sys.stdout)

