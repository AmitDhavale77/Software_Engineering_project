import os
import socket
import urllib.request

from database import Database
from parser import HL7MessageParser
from model_class import AKIPredictor
import simulator
import logging


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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    logger.info("Starting system")

    MLLP_HOST, MLLP_PORT = os.getenv("MLLP_ADDRESS").split(":")
    MLLP_PORT = int(MLLP_PORT)
    PAGER_HOST, PAGER_PORT = os.getenv("PAGER_ADDRESS").split(":")
    PAGER_PORT = int(PAGER_PORT)

    parser = HL7MessageParser()
    db = Database()
    db.populate_history("/data/history.csv")
    logger.info("Database loaded successfully.")

    predictor = AKIPredictor("/simulator/scaler.pkl", "/simulator/xgb_model.pkl")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((MLLP_HOST, MLLP_PORT))
        buffer = b""  # Buffer to store incomplete messages

        while True:
            data = s.recv(MLLP_BUFFER_SIZE)
            if len(data) == 0:
                break

            buffer += data  # Append received data to buffer
            messages, buffer = simulator.parse_mllp_messages(buffer, "")

            for message in messages:
                msg, fields = parser.parse(message.decode("utf-8"))
                mrn = fields["mrn"]

                if msg == "PAS_admit":
                    db.write_pas_data(**fields)
                    logger.info(f"PAS stored successfully for MRN: {mrn}, {fields}")
                elif msg == "LIMS":
                    for obs in fields["results"]:
                        db.write_lims_data(mrn, **obs)
                    logger.info(f"LIMS data stored successfully for MRN: {mrn}")

                if msg == "LIMS":
                    data = db.fetch_data(mrn)
                    preds = predictor.predict(data)
                    logger.info(f"Prediction: {preds[0]}, made for MRN: {mrn}, timestamp: {preds[2]}")
                    if preds[0] == 1:
                        data = f"{mrn},{preds[2].strftime("%Y%m%d%H%M%S")}"
                        r = urllib.request.urlopen(f"http://{PAGER_HOST}:{PAGER_PORT}/page", data=data.encode("utf-8"))
                s.sendall(to_mllp(ACK))
                logger.info("Acknowledgement sent")
    db.close()
