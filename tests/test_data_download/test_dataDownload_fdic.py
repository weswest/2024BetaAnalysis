import unittest
import pandas as pd
from src.data_download.dataDownload_fdic import get_financial_field_value

class TestFDICDownload(unittest.TestCase):

    def test_get_financial_field_value(self):
        # Mock response for testing
        report_date = "20231231"
        cert_id = 628
        field_name = "DEPDOM"
        expected_value = 2037915000  # The expected value you want to test against
        
        # Since this is a unit test, you might want to mock the requests.get call
        # Here we just test the function signature and mock return value conceptually
        
        actual_value = get_financial_field_value(report_date, cert_id, field_name)
        # Add your mock setup here and assertions
        self.assertEqual(actual_value, expected_value)

if __name__ == "__main__":
    unittest.main()
