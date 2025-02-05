#!/usr/bin/env python3
'''

After analyzing the dataset, I observed that it is both non-linear and time-series in nature. 
My initial thought was to use a Decision Tree, which yielded an F3-score of approximately 
0.91—a fairly strong performance. However, I wanted to explore other models to see if I could 
achieve a better score while still effectively capturing both the non-linearity and the time-series 
characteristics. 
his led me to use XGBoost, a powerful and efficient gradient boosting library. It leverages the 
concept of boosting, where multiple weak learners (simple DTs) are sequentially trained. Each 
subsequent tree learns from the errors of its predecessors, gradually improving the overall model's 
accuracy. Also, combining multiple trees, XGBoost can effectively reduce both bias and variance.
Thus, I decided to use XGBoost for this task. 

'''
import argparse
import csv
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
import pickle


'''
Could only update the code when the message receiver part is fixed - Amit
'''

# Function to preprocess and transform the dataset
def preprocess_and_transform(data, flag=False):
    """
    This function preprocesses the input data by:
    1. Converting date columns to datetime format.
    2. Creating dummy variables for categorical columns.
    3. Calculating features based on creatinine results.
    4. Filtering relevant columns for training/testing.

    Args:
        data (pd.DataFrame): Input data
        flag (bool): Indicates if the data is for testing (default: False)

    Returns:
        pd.DataFrame: Preprocessed and filtered data
    """

    # Step 1: Convert 'creatinine_date' columns to datetime format
    for col in data.columns:
        if 'creatinine_date' in col:
            data[col] = pd.to_datetime(data[col], errors='coerce')

    # Step 2: Handle categorical variables and encode them as dummy variables
    if flag: # Testing data

        data = pd.get_dummies(data, columns=['sex'], drop_first=False)
        data = data.drop(columns=['sex_m'])
        data['sex_f'] = data['sex_f'].astype(int)
    
    else: # Training data

        data = pd.get_dummies(data, columns=['aki', 'sex'], drop_first=False)
        data = data.drop(columns=['sex_m', 'aki_n'])
        data['aki_y'] = data['aki_y'].astype(int)
        data['sex_f'] = data['sex_f'].astype(int)

    # Step 3: Calculate 'min_of_min_ls', 'median_of_median_ls', and 'most_recent_creatinine_result'

    max_creatinine_index = max(
        int(col.split('_')[-1]) for col in data.columns if col.startswith('creatinine_result_')
    )

    min_values = []
    median_values = []
    latest_result = []
    for index, row in data.iterrows():
        min_ls = []
        median_ls = []
        
        for i in range(max_creatinine_index + 1):
            creatinine_col = f'creatinine_result_{i}'

            if not pd.notna(row[creatinine_col]):
                break
            latest_val = row[creatinine_col]
            min_ls.append(row[creatinine_col])
            median_ls.append(row[creatinine_col])
        latest_result.append(latest_val)
        min_values.append(np.min(min_ls) if min_ls else 0)
        median_values.append(np.median(median_ls) if median_ls else 0)

    data['min_of_min_ls'] = min_values
    data['median_of_median_ls'] = median_values
    data['most_recent_creatinine_result'] = latest_result

    # Step 4: Filter the DataFrame to keep only relevant columns
    if flag: # Columns for testing data
        columns_to_keep = ['min_of_min_ls', 'median_of_median_ls', 'age', 'sex_f', 'most_recent_creatinine_result']
    
    else: # Columns for training data
        columns_to_keep = ['min_of_min_ls', 'median_of_median_ls', 'age', 'aki_y', 'sex_f', 'most_recent_creatinine_result']

    filtered_data = data[columns_to_keep]
    return filtered_data


def make_predictions():

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="test.csv")
    # parser.add_argument("--output", default="aki.csv")
    flags = parser.parse_args()

    # w = csv.writer(open(flags.output, "w"))
    # w.writerow(("aki",))

    # # Load training and testing datasets
    # training_data = pd.read_csv("data//training.csv")
    test_data = pd.read_csv(flags.input)

    # Preprocess training and testing data
    # train_data = preprocess_and_transform(training_data)
    test_data = preprocess_and_transform(test_data, flag=True)
    
    # Standardize numerical columns
    

    # Load the scaler from the file
    with open("scaler.pkl", "rb") as file:
        loaded_scaler = pickle.load(file)

    columns_to_scale = ['age', 'min_of_min_ls', 'median_of_median_ls', 'most_recent_creatinine_result']

   
    test_data[columns_to_scale] = loaded_scaler.transform(test_data[columns_to_scale])


    # Features for testing data
    X_test = test_data

    # Initialize and train the XGBoost classifier

    # Drop 'aki' column from X_test if it exists (To handle the case when 'aki' is present in the testing data)
    if 'aki' in X_test.columns:
        X_test = X_test.drop(columns=['aki'])

    with open("xgb_model.pkl", "rb") as file:
        xgb_model = pickle.load(file)
       
    # Predict AKI outcomes for test data
    y_pred_svc = xgb_model.predict(X_test)
 
    # Write predictions to output file
    for ele in y_pred_svc:
        if ele == 0:
            # w.writerow(['n'])
            return 'n'
        else:
            # w.writerow(['y'])
            return 'y'
       
                
if __name__ == "__main__":
    make_predictions()