import pickle
import numpy as np
from datetime import datetime


class AKIPredictor:
    def __init__(self, model_path="xgb_model.pkl"):
        with open(model_path, "rb") as f:
            self.model = pickle.load(f)

    def preprocess_and_transform(self, input_dict):
        dob = datetime.fromisoformat(input_dict["dob"])

        dates = input_dict["dates"]
        latest_date = datetime.fromisoformat(dates[-1])
        age = (latest_date - dob).days // 365

        sex = input_dict["sex"]
        creatinine_levels = input_dict["creatinine_levels"]

        latest_creatinine = creatinine_levels[-1]
        rv1 = latest_creatinine / np.min(creatinine_levels)
        rv2 = latest_creatinine / np.median(creatinine_levels)

        new_data = np.asarray([age, sex, latest_creatinine, rv1, rv2])
        return new_data, latest_date

    def predict(self, data):
        processed_data, latest_date = self.preprocess_and_transform(data)
        y_pred = self.model.predict(processed_data[None, :])[0]
        return y_pred, latest_date
