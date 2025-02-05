#!/usr/bin/env python3
import argparse
import csv
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
import pickle

class AKIPredictor:
    def __init__(self, scaler_path="scaler.pkl", model_path="xgb_model.pkl"):
        """
        Initialize the predictor by loading the scaler and XGBoost model.
        """
        # Load the scaler from file
        with open(scaler_path, "rb") as f:
            self.scaler = pickle.load(f)
        # Load the XGBoost model from file
        with open(model_path, "rb") as f:
            self.model = pickle.load(f)
        
        # List of numerical columns to scale
        self.columns_to_scale = ['age', 'min_of_min_ls', 'median_of_median_ls', 'most_recent_creatinine_result']

    def preprocess_and_transform(self, input_dict, flag=False):
        """
        Preprocess the input data by:
        1. Converting date columns to datetime format.
        2. Creating dummy variables for categorical columns.
        3. Calculating features based on creatinine results.
        4. Filtering relevant columns for training/testing.
        
        Args:
            data (pd.DataFrame): Input data.
            flag (bool): If True, process as testing data (default: False).
        
        Returns:
            pd.DataFrame: Preprocessed and filtered data.
        """
        import pandas as pd
        import numpy as np

        mrn = input_dict.get('mrn', None)
        # Step 1: Calculate age from date of birth (dob)
        dob = pd.to_datetime(input_dict.get('dob'), errors='coerce')

        dates = input_dict.get('dates', [])
        if dates:
            latest_date = pd.to_datetime(dates[-1], errors='coerce')
        else:
            latest_date = pd.Timestamp.today()  # Fallback if 'dates' is empty.  
        
        age = (latest_date - dob).days / 365.25  
        age = int(age)

        sex_f = int(input_dict.get('sex', 0))
        # Step 3: Process creatinine levels to compute the required features.
        creatinine_levels = input_dict.get('creatinine_levels', [])

        if creatinine_levels:
            min_of_min_ls = min(creatinine_levels)
            median_of_median_ls = float(np.median(creatinine_levels))
            most_recent_creatinine_result = creatinine_levels[-1]
        else:
            min_of_min_ls = 0
            median_of_median_ls = 0
            most_recent_creatinine_result = 0
        
        # Build the new data record.
        new_data = {
            'min_of_min_ls': min_of_min_ls,
            'median_of_median_ls': median_of_median_ls,
            'age': age,
            'sex_f': sex_f,
            'most_recent_creatinine_result': most_recent_creatinine_result
        }        

        # For training data (flag=False), include the 'aki_y' field if provided.
        if not flag:
            aki_val = input_dict.get('aki', None)
            if aki_val is not None:
                aki_y = 1 if aki_val in ['y', 'Y', 1, '1'] else 0
            else:
                aki_y = 0
            new_data['aki_y'] = aki_y

        if flag:
            columns_to_keep = ['min_of_min_ls', 'median_of_median_ls', 'age', 'sex_f', 'most_recent_creatinine_result']
        else:
            columns_to_keep = ['min_of_min_ls', 'median_of_median_ls', 'age', 'aki_y', 'sex_f', 'most_recent_creatinine_result']

        filtered_data = pd.DataFrame([new_data])[columns_to_keep]
        return filtered_data, mrn, latest_date

    def predict(self, data):
        """
        Make AKI predictions for the given data.
        
        Args:
            data (pd.DataFrame): Raw input data (expected as testing data).
        
        Returns:
            list: A list of predictions ('n' for 0 and 'y' for 1).
        """
        # Preprocess and transform the data (using flag=True for testing)
        processed_data, mrn, latest_date = self.preprocess_and_transform(data, flag=True)

        # Standardize numerical columns using the loaded scaler.
        processed_data[self.columns_to_scale] = self.scaler.transform(processed_data[self.columns_to_scale])

        # Drop 'aki' column if it exists (to avoid interfering with prediction)
        if 'aki' in processed_data.columns:
            processed_data = processed_data.drop(columns=['aki'])

        # Predict using the loaded XGBoost model
        y_pred = self.model.predict(processed_data)

        # Convert numeric predictions to 'n' or 'y'
        predictions = ['n' if pred == 0 else 'y' for pred in y_pred]
        return predictions, mrn, latest_date

    def predict_from_db(self, input_dict):
        """
        Load data from a CSV file and make predictions.
        
        Args:
            input_file (str): Path to the input CSV file.
        
        Returns:
            list: A list of predictions.
        """
        # {‘mrn’: 10001, ‘dob’: timestamp, ‘sex’: 0, ‘dates’: [], ‘creatinine_levels’: []}

        # self.send_response(http.HTTPStatus.OK)
        # self.send_header("Content-Type", "text/plain")
        # self.end_headers()
        # self.wfile.write(b"ok\n")        
       
        return self.predict(input_dict)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="test.csv", help="Input CSV file for testing")
    # Uncomment and adjust the following line if you wish to write predictions to an output file.
    # parser.add_argument("--output", default="aki.csv", help="Output CSV file for predictions")
    args = parser.parse_args()

    predictor = AKIPredictor()
    predictions = predictor.predict_from_file(args.input)

    # For demonstration purposes, print each prediction.
    for pred in predictions:
        print(pred)


if __name__ == "__main__":
    main()
