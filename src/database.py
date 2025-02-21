import os
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime


class Database:
    def __init__(self, pat_db_name="patients.db", tests_db_name="blood_tests.db"):
        self.db_exists = os.path.exists(pat_db_name)

        self.pat_db = sqlite3.connect(pat_db_name)
        self.pat_cur = self.pat_db.cursor()

        self.tests_db = sqlite3.connect(tests_db_name)
        self.tests_cur = self.tests_db.cursor()

        if not self.db_exists:
            self.pat_cur.execute("CREATE TABLE patients(mrn, dob, sex)")
            self.tests_cur.execute("CREATE TABLE blood_tests(mrn, timestamp, creatinine_level)")

    def populate_history(self, history_csv_path):
        if self.db_exists:
            return

        hist = pd.read_csv(history_csv_path)
        hist_rows = []

        for _, row in hist.iterrows():
            row = row[~pd.isnull(row)]
            mrn = row["mrn"]
            date_creatinine = row.values[1:].reshape(-1, 2)
            dates = list(map(datetime.fromisoformat, date_creatinine[:, 0]))
            creatinine_levels = date_creatinine[:, 1].astype(np.float32)
            for date, creatinine_level in zip(dates, creatinine_levels):
                hist_rows.append(f"({mrn}, '{date}', {creatinine_level})")

        self.tests_cur.execute(f"""
            INSERT INTO blood_tests VALUES {", ".join(hist_rows)}
        """)
        self.tests_db.commit()

    def write_pas_data(self, mrn, dob, sex):
        self.pat_cur.execute(f"""
            INSERT INTO patients VALUES ({mrn}, '{dob}', {sex})
        """)
        self.pat_db.commit()

    def write_lims_data(self, mrn, date, result):
        self.tests_cur.execute(f"""
            INSERT INTO blood_tests VALUES ({mrn}, '{date}', {result})
        """)
        self.tests_db.commit()

    def read_pas_data(self, mrn):
        res = self.pat_cur.execute(
            f"SELECT * FROM patients WHERE mrn={mrn} ORDER BY rowid DESC"
        )
        return res.fetchone()

    def read_lims_data(self, mrn):
        res = self.tests_cur.execute(f"SELECT * FROM blood_tests WHERE mrn={mrn}")
        return res.fetchall()

    def fetch_data(self, mrn):
        pas_data = self.read_pas_data(mrn)
        lims_data = self.read_lims_data(mrn)
        data = {
            "mrn": pas_data[0],
            "dob": pas_data[1],
            "sex": pas_data[2],
            "dates": [ld[1] for ld in lims_data],
            "creatinine_levels": [ld[2] for ld in lims_data],
        }
        return data

    def close(self):
        self.pat_db.close()
        self.tests_db.close()


if __name__ == "__main__":
    pas_messages = [
        ("149539321", "19860417", 0),
        ("124674001", "20230416", 1),
        ("186512977", "20180109", 0),
    ]
    lims_messages = [
        ("153541819", "20240411055800", 104.50414808079834),
        ("153541819", "20240411060800", 170.21986290958355),
        ("124674001", "20240218071300", 109.10220038311532),
        ("186512977", "20240605111400", 113.48685810736936),
        ("186512977", "20240605112800", 135.39630713592294),
        ("186512977", "20240605113500", 158.48822434796762),
        ("186512977", "20240605113800", 102.66797910333874),
    ]
    db = Database()
    db.populate_history("history.csv")
    for msg in pas_messages:
        db.write_pas_data(*msg)

    for msg in lims_messages:
        db.write_lims_data(*msg)

    print(db.fetch_data(mrn="149539321"))
    print(db.fetch_data(mrn="186512977"))
    db.close()
