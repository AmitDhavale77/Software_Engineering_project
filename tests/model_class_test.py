import unittest
from unittest.mock import patch, mock_open
import pickle
import numpy as np
import pandas as pd
from model.model_class import AKIPredictor  # Import your class

class TestAKIPredictor(unittest.TestCase):
    
    @patch("builtins.open", new_callable=mock_open, read_data=pickle.dumps(None))
    @patch("pickle.load")
    def setUp(self, mock_pickle_load, mock_open):
        """Setup mock model and scaler for tests."""
        self.mock_scaler = unittest.mock.Mock()
        self.mock_scaler.transform = lambda x: x  # Mock transform method to return input

        self.mock_model = unittest.mock.Mock()
        self.mock_model.predict.return_value = np.array([1])  # Mock prediction

        mock_pickle_load.side_effect = [self.mock_scaler, self.mock_model]  # Mock loading order
        
        self.predictor = AKIPredictor()

    def test_preprocess_and_transform(self):
        """Test preprocessing function with sample input."""
        input_data = {
            'mrn': 1001,
            'dob': "1980-05-15",
            'sex': 1,
            'dates': ["2023-01-01", "2023-06-15"],
            'creatinine_levels': [1.0, 1.2, 1.4]
        }
        processed_data, mrn, latest_date = self.predictor.preprocess_and_transform(input_data, flag=True)

        self.assertEqual(mrn, 1001)
        self.assertEqual(latest_date, pd.Timestamp("2023-06-15"))
        self.assertIn('age', processed_data.columns)
        self.assertIn('sex_f', processed_data.columns)
        self.assertIn('min_of_min_ls', processed_data.columns)

    def test_predict(self):
        """Test prediction output format."""
        input_data = {
            'mrn': 1001,
            'dob': "1980-05-15",
            'sex': 1,
            'dates': ["2023-01-01", "2023-06-15"],
            'creatinine_levels': [1.0, 1.2, 1.4]
        }
        predictions, mrn, latest_date = self.predictor.predict(input_data)

        self.assertEqual(predictions, ['y'])
        self.assertEqual(mrn, 1001)
        self.assertEqual(latest_date, pd.Timestamp("2023-06-15"))

    def test_predict_from_db(self):
        """Test predict_from_db method."""
        input_data = {
            'mrn': 1002,
            'dob': "1995-10-25",
            'sex': 0,
            'dates': ["2023-02-10"],
            'creatinine_levels': [0.9]
        }
        predictions, mrn, latest_date = self.predictor.predict_from_db(input_data)

        self.assertEqual(predictions, ['y'])
        self.assertEqual(mrn, 1002)
        self.assertEqual(latest_date, pd.Timestamp("2023-02-10"))

if __name__ == '__main__':
    unittest.main()
