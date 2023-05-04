"""
Sample unit testing for the sample cloud function
"""

import unittest
from main import cloud_function_template


class TestCloudFunctionTemplate(unittest.TestCase):
    """
    Test suite for the cloud function template
    """

    def test_cloud_function_template(self):
        data = "some_data"
        assert "some_data" in cloud_function_template(data)
