import pandas as pd
import os
from src.database import Database
from src.parser import HL7MessageParser
from model.model_class import AKIPredictor
from src.simulator import read_hl7_messages


if __name__ == "__main__":
    hl7_messages = read_hl7_messages("messages.mllp")

    parser = HL7MessageParser()
    db = Database()
    predictor = AKIPredictor("model\\scaler.pkl", "model\\xgb_model.pkl")
    outputs = []

    for message in hl7_messages:
        msg, fields = parser.parse(message.decode("utf-8"))

        if msg == "PAS_admit":
            db.write_pas_data(**fields)
        elif msg == "LIMS":
            db.write_lims_data(**fields)
        else:
            continue

        if msg == "LIMS":
            mrn = fields["mrn"]
            data = db.fetch_data(mrn)
            preds = predictor.predict(data)
            if preds[0][0] == "y":
                outputs.append([mrn, preds[2]])

    output = pd.DataFrame(outputs, columns=["mrn", "timestamp"])
    output.to_csv("pred_aki.csv", index=False)
    # os.remove('patients.db')
    # os.remove('blood_tests.db')
