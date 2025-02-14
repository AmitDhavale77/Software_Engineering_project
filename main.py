import pandas as pd
from tqdm import tqdm

from src.database import Database
from src.parser import HL7MessageParser
from src.simulator import read_hl7_messages
from model.model_class import AKIPredictor


if __name__ == "__main__":
    hl7_messages = read_hl7_messages("messages.mllp")

    parser = HL7MessageParser()
    db = Database()
    predictor = AKIPredictor("model/scaler.pkl", "model/xgb_model.pkl")
    outputs = []

    for message in tqdm(hl7_messages[:5000]):
        print(f"message received: {message}")
        msg, fields = parser.parse(message.decode("utf-8"))
        mrn = fields["mrn"]

        if msg == "PAS_admit":
            db.write_pas_data(**fields)
            print("PAS message saved")
        elif msg == "LIMS":
            for obs in fields["results"]:
                db.write_lims_data(mrn, **obs)
            print("LIMS saved")
        else:
            continue

        if msg == "LIMS":
            print("LIMS predictor")
            data = db.fetch_data(mrn)
            preds = predictor.predict(data)
            if preds[0] == 1:
                outputs.append([mrn, preds[2]])
            print("")
            print(f"prediction: {preds[0]}")
            print("prediction saved")

    output = pd.DataFrame(outputs, columns=["mrn", "timestamp"])
    output.to_csv("pred_aki.csv", index=False)
    # os.remove('patients.db')
    # os.remove('blood_tests.db')
