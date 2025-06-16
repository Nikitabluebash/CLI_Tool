import unittest
from unittest.mock import patch
import pandas as pd
from enhancedata import validate_and_enrich

class TestEnhancedataValidation(unittest.TestCase):

    @patch("enhancedata.fetch_coordinates")
    def test_realistic_row_from_csv(self, mock_fetch_coords):
        # Simulate successful geocoding
        mock_fetch_coords.return_value = (-37.8136, 144.9631)  # Melbourne coordinates

        # Mock input as per your actual CSV format
        test_df = pd.DataFrame([{
            "Email": "jane.doe@example.com",
            "First Name": "Jane",
            "Last Name": "Doe",
            "Residential Address Street": "123 Queen St",
            "Residential Address Locality": "Melbourne",
            "Residential Address Postcode": "3000",
            "Postal Address Street": "",
            "Postal Address Locality": "",
            "Postal Address Postcode": ""
        }])

        # Run validation
        result_df = validate_and_enrich(test_df)

        # Assertions
        self.assertEqual(len(result_df), 1, "Valid row should be retained")
        self.assertAlmostEqual(result_df.iloc[0]["latitude"], -37.8136)
        self.assertAlmostEqual(result_df.iloc[0]["longitude"], 144.9631)
        self.assertEqual(result_df.iloc[0]["Email"], "jane.doe@example.com")
        self.assertEqual(result_df.iloc[0]["First Name"], "Jane")

if __name__ == "__main__":
    unittest.main()
