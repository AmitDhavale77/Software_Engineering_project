import os
import gc
import sys
import time
import pickle
import socket
import signal
import logging
import argparse
import urllib.request
from copy import deepcopy
from threading import Thread

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

if os.path.isfile("/state/lims_queue.pkl"):
    with open("/state/lims_queue.pkl", "rb") as f:
        lims_queue = pickle.load(f)
    with open("/state/pager_queue.pkl", "rb") as f:
        pager_queue = pickle.load(f)
else:
    lims_queue, pager_queue = [], []


def process_lims_queue(predictor, logger):
    while True:
        if not lims_queue:
            continue

        lims_queue_copy = deepcopy(lims_queue)
        db_copy = Database("/state/patients.db", "/state/blood_tests.db")

        for lims_data in lims_queue_copy:
            mrn, timestamp = lims_data
            data = db_copy.fetch_data(mrn, timestamp)
            if data is None:
                continue

            y_pred, test_date = predictor.predict(data)
            logger.info(f"LIMS Queue, Prediction: {y_pred}, made for MRN: {mrn}, timestamp: {timestamp}")
            if y_pred == 1:
                pager_data = f"{mrn},{test_date.strftime('%Y%m%d%H%M%S')}".encode("utf-8")
                pager_queue.append(pager_data)

            lims_queue.remove(lims_data)

        db_copy.close()
        del lims_queue_copy, db_copy
        gc.collect()


def process_pager_queue(pager_host, pager_port, logger):
    while True:
        if not pager_queue:
            continue

        pager_queue_copy = deepcopy(pager_queue)
        for pager_data in pager_queue_copy:
            try:
                urllib.request.urlopen(f"http://{pager_host}:{pager_port}/page", timeout=1, data=pager_data)
                logger.info(f"Pager queue, Pager request sent successfully for {pager_data.decode('utf-8')}")
            except:
                continue
            pager_queue.remove(pager_data)

        del pager_queue_copy
        gc.collect()


def connect_to_mllp_server(host, port, logger):
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

    s = connect_to_mllp_server(MLLP_HOST, MLLP_PORT, logger)
    buffer = b""  # Buffer to store incomplete messages

    def graceful_shutdown(signum, frame):
        logger.info("Shutting down system.")
        db.close()
        s.close()
        with open("/state/lims_queue.pkl", "wb") as f:
            pickle.dump(lims_queue, f)
        with open("/state/pager_queue.pkl", "wb") as f:
            pickle.dump(pager_queue, f)
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)

    lims_queue_thread = Thread(target=process_lims_queue, args=(predictor, logger), daemon=True)
    lims_queue_thread.start()

    pager_queue_thread = Thread(target=process_pager_queue, args=(PAGER_HOST, PAGER_PORT, logger), daemon=True)
    pager_queue_thread.start()

    while True:
        try:
            data = s.recv(simulator.MLLP_BUFFER_SIZE)
        except Exception as e:
            logger.warning(f"MLLP connection failed: {e}. Reconnecting")
            s.close()
            s = connect_to_mllp_server(MLLP_HOST, MLLP_PORT, logger)
            continue

        if len(data) == 0:
            logger.warning("MLLP connection closed by peer. Reconnecting")
            s.close()
            s = connect_to_mllp_server(MLLP_HOST, MLLP_PORT, logger)
            continue

        buffer += data  # Append received data to buffer
        try:
            messages, buffer = simulator.parse_mllp_messages(buffer, "")
        except Exception as e:
            logger.warning(f"Couldn't parse buffer: {buffer} due to exception: {e}")
            messages = []

        for message in messages:
            messages_counter.inc()  # increment counter
            msg, fields, status = msg_parser.parse(message.decode("utf-8"))
            if status == "error":
                logger.warning(f"Couldn't parse message: {message}")
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
                for obs in fields["results"]:
                    timestamp = obs["date"]
                    data = db.fetch_data(mrn, timestamp)
                    if data is None:
                        logger.warning("Couldn't find PAS data. Added to LIMS queue")
                        lims_queue.append((mrn, timestamp))
                        continue

                    y_pred, test_date = predictor.predict(data)
                    logger.info(f"Prediction: {y_pred}, made for MRN: {mrn}, timestamp: {timestamp}")

                    if y_pred == 1:
                        pos_counter.inc()
                        pager_data = f"{mrn},{test_date.strftime('%Y%m%d%H%M%S')}".encode("utf-8")
                        try:
                            urllib.request.urlopen(f"http://{PAGER_HOST}:{PAGER_PORT}/page", timeout=1, data=pager_data)
                            logger.info(f"Pager request sent successfully for MRN: {mrn}")
                        except Exception as e:
                            logger.warning(f"Pager request failed: {e}. Added to pager queue")
                            pager_queue.append(pager_data)

        ack = create_acknowledgement("AA")
        while True:
            try:
                s.sendall(ack)
                logger.info("Acknowledgement sent")
                break
            except Exception as e:
                logger.warning(f"MLLP connection failed: {e}. Reconnecting")
                s.close()
                s = connect_to_mllp_server(MLLP_HOST, MLLP_PORT, logger)
