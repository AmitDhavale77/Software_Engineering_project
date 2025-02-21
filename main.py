import os
import pandas as pd
from tqdm import tqdm

from src.database import Database
from src.parser import HL7MessageParser
from src.simulator import read_hl7_messages
from model.model_class import AKIPredictor
from prometheus_client import start_http_server, Counter



messages_counter = Counter('messaged_received', 'Number of messages received') 
lims_counter = Counter('blood_test_received', 'Number of LIMs messages receieved')


if __name__ == "__main__":
    start_http_server(8000)
    hl7_messages = read_hl7_messages("messages.mllp")

    parser = HL7MessageParser()
    db = Database()
    db.populate_history("history.csv")
    predictor = AKIPredictor("model/xgb_model.pkl")
    outputs = []

    for message in tqdm(hl7_messages[:5000]):
        messages_counter.inc() # increment counter
        msg, fields = parser.parse(message.decode("utf-8"))
        mrn = fields["mrn"]

        if msg == "PAS_admit":
            db.write_pas_data(**fields)
        elif msg == "LIMS":
            for obs in fields["results"]:
                db.write_lims_data(mrn, **obs)
        else:
            continue

        if msg == "LIMS":
            lims_counter.inc()
            data = db.fetch_data(mrn)
            preds = predictor.predict(data)
            if preds[0] == 1:
                outputs.append([mrn, preds[2]])

    output = pd.DataFrame(outputs, columns=["mrn", "timestamp"])
    output.to_csv("pred_aki.csv", index=False)
    os.remove("patients.db")
    os.remove("blood_tests.db")
