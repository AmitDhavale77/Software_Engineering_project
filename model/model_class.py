import pickle
import numpy as np
from datetime import datetime


class AKIPredictor:
    def __init__(self, scaler_path="scaler.pkl", model_path="xgb_model.pkl"):
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)

        self.mean = scaler.mean_[[1, 2, 0, 3]]
        self.mean = np.insert(self.mean, 3, 0)
        self.std = scaler.scale_[[1, 2, 0, 3]]
        self.std = np.insert(self.std, 3, 1)

        with open(model_path, "rb") as f:
            self.model = pickle.load(f)

    def preprocess_and_transform(self, input_dict):
        mrn = input_dict["mrn"]
        dob = datetime.fromisoformat(input_dict["dob"])

        dates = input_dict["dates"]
        latest_date = datetime.fromisoformat(dates[-1])
        age = (latest_date - dob).days // 365

        sex = input_dict["sex"]
        creatinine_levels = input_dict["creatinine_levels"]

        min_ls = min(creatinine_levels)
        median_ls = float(np.median(creatinine_levels))
        most_recent_creatinine_result = creatinine_levels[-1]

        new_data = np.asarray(
            [min_ls, median_ls, age, sex, most_recent_creatinine_result]
        )
        return new_data, mrn, latest_date

    def predict(self, data):
        processed_data, mrn, latest_date = self.preprocess_and_transform(data)
        processed_data = (processed_data - self.mean) / self.std
        y_pred = self.model.predict(processed_data[None, :])[0]
        return y_pred, mrn, latest_date
