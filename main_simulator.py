import os
import socket
import urllib.request

from database import Database
from parser import HL7MessageParser
from model_class import AKIPredictor
import simulator


MLLP_BUFFER_SIZE = 1024

ACK = [
    r"MSH|^~\&|||||20240129093837||ACK|||2.5",
    r"MSA|AA",
]

def to_mllp(segments):
    m = bytes(chr(simulator.MLLP_START_OF_BLOCK), "ascii")
    m += bytes("\r".join(segments) + "\r", "ascii")
    m += bytes(chr(simulator.MLLP_END_OF_BLOCK) + chr(simulator.MLLP_CARRIAGE_RETURN), "ascii")
    return m

def from_mllp(buffer):
    return str(buffer[1:-3], "ascii").split("\r") # Strip MLLP framing and final \r

if __name__ == "__main__":
    MLLP_HOST, MLLP_PORT = os.getenv("MLLP_ADDRESS").split(":")
    MLLP_PORT = int(MLLP_PORT)
    PAGER_HOST, PAGER_PORT = os.getenv("PAGER_ADDRESS").split(":")
    PAGER_PORT = int(PAGER_PORT)

    parser = HL7MessageParser()
    db = Database()
    db.populate_history("/data/history.csv")
    predictor = AKIPredictor("/simulator/scaler.pkl", "/simulator/xgb_model.pkl")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MLLP_HOST, MLLP_PORT))
        while True:
            buffer = s.recv(MLLP_BUFFER_SIZE)
            if len(buffer) == 0:
                break
            message = from_mllp(buffer)
            msg, fields = parser.parse(message.decode("utf-8"))
            mrn = fields["mrn"]

            if msg == "PAS_admit":
                db.write_pas_data(**fields)
            elif msg == "LIMS":
                for obs in fields["results"]:
                    db.write_lims_data(mrn, **obs)

            if msg == "LIMS":
                data = db.fetch_data(mrn)
                preds = predictor.predict(data)
                if preds[0][0] == "y":
                    data = f"{mrn},{preds[2].strftime("%Y%m%d%H%M%S")}"
                    r = urllib.request.urlopen(f"http://{PAGER_HOST}:{PAGER_PORT}/page", data=data.encode("utf-8"))
            s.sendall(to_mllp(ACK))
    db.close()
