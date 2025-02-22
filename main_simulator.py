import os
import sys
import time
import socket
import signal
import logging
import argparse
import urllib.request

from database import Database
from parser import HL7MessageParser
from model_class import AKIPredictor
from acknowledgements import create_acknowledgement
from prometheus_client import start_http_server, Counter

import simulator


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

messages_counter = Counter('messaged_received', 'Number of messages received') 
lims_counter = Counter('blood_test_received', 'Number of LIMs messages receieved')
mllp_counter = Counter('mllp_connections_made', 'Number of connections to the MLLP socket')
http_counter = Counter('failed_http', 'Number of times the pager HTTP request failed')
pos_counter = Counter('pos_predictions', 'Number of positive AKI predictions made')


MLLP_RETRY_SECONDS = 1


def connect_to_mllp_server(host, port):
    while True:
        mllp_counter.inc()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            logger.info("Connected to MLLP server")
            break
        except Exception as e:
            logger.warning(f"MLLP connection failed: {e}. Retrying in {MLLP_RETRY_SECONDS}s")
            time.sleep(MLLP_RETRY_SECONDS)
    return s


if __name__ == "__main__":
    start_http_server(8000)
    logger = logging.getLogger(__name__)
    logger.info("Starting system")

    MLLP_HOST, MLLP_PORT = os.getenv("MLLP_ADDRESS").split(":")
    MLLP_PORT = int(MLLP_PORT)
    PAGER_HOST, PAGER_PORT = os.getenv("PAGER_ADDRESS").split(":")
    PAGER_PORT = int(PAGER_PORT)

    parser = argparse.ArgumentParser()
    parser.add_argument("--history", default="/data/history.csv", help="Path to history.csv")
    flags = parser.parse_args()

    msg_parser = HL7MessageParser()
    db = Database("/state/patients.db", "/state/blood_tests.db")
    db.populate_history(flags.history)
    logger.info("Database loaded successfully.")

    predictor = AKIPredictor("/simulator/xgb_model.pkl")

    s = connect_to_mllp_server(MLLP_HOST, MLLP_PORT)
    buffer = b""  # Buffer to store incomplete messages

    def graceful_shutdown(signum, frame):
        logger.info("Shutting down system.")
        db.close()
        s.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)

    while True:
        data = s.recv(simulator.MLLP_BUFFER_SIZE)
        if len(data) == 0:
            logger.warning("MLLP connection closed by peer. Reconnecting")
            s.close()
            s = connect_to_mllp_server(MLLP_HOST, MLLP_PORT)
            continue

        buffer += data  # Append received data to buffer
        messages, buffer = simulator.parse_mllp_messages(buffer, "")
        for message in messages:
            messages_counter.inc() # increment counter
            msg, fields, status = msg_parser.parse(message.decode("utf-8"))
            if status == "error":
                s.sendall(create_acknowledgement("AA"))
                logger.info("Acknowledgement sent")
                continue
            mrn = fields["mrn"]

            if msg == "PAS_admit":
                db.write_pas_data(**fields)
            elif msg == "LIMS":
                lims_counter.inc()
                for obs in fields["results"]:
                    db.write_lims_data(mrn, **obs)

            logger.info(f"{msg} message parsed successfully for MRN: {mrn}")
            logger.debug(f"Parsed fields: {fields}")

            if msg == "LIMS":
                data = db.fetch_data(mrn)
                preds = predictor.predict(data)
                logger.info(f"Prediction: {preds[0]}, made for MRN: {mrn}, timestamp: {preds[2]}")


                if preds[0] == 1:
                    pos_counter.inc()
                    pager_data = f"{mrn},{preds[2].strftime('%Y%m%d%H%M%S')}".encode("utf-8")
                    while True:
                        try:
                            r = urllib.request.urlopen(f"http://{PAGER_HOST}:{PAGER_PORT}/page", data=pager_data)
                            logger.info(f"Pager request sent successfully for MRN: {mrn}")
                            break
                        except Exception as e:
                            logger.warning(f"Pager request failed: {e}. Retrying in {MLLP_RETRY_SECONDS}s")
                            time.sleep(MLLP_RETRY_SECONDS)
                            
            s.sendall(create_acknowledgement("AA"))
            logger.info("Acknowledgement sent")

