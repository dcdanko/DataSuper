"""Test persistent classes."""

import os
import tempfile
import unittest
from shutil import rmtree


class BaseTestDataSuper(unittest.TestCase):
    """Test persistent classes."""

    def setUp(self):
        tdir = tempfile.mkdtemp()
        os.chdir(tdir)
        self.tdir = tdir

    def tearDown(self):
        rmtree(self.tdir)
