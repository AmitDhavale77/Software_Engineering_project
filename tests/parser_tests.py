import unittest
from parser import HL7MessageParser

class TestHL7MessageParser(unittest.TestCase):

    def setUp(self):
        self.parser = HL7MessageParser()

    def test_pas_admit(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240107133000||ADT^A01|||2.5\r"
            "PID|1||185620675||KAYLA HENRY||20211106|F\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[0], 'PAS_admit')


    def test_other_message(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240107133000||ADT^A08|||2.5\r"
            "PID|1||185620675||KAYLA HENRY||20211106|F\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message, 'OTHER')

    
    def test_pas_discharge(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331054700||ADT^A03|||2.5\r"
            "PID|1||112034143\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[0], 'PAS_discharge')


    def test_lims(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331005400||ORU^R01|||2.5\r"
            "PID|1||157828764\r"
            "OBR|1||||||20240331005400\r"
            "OBX|1|SN|CREATININE||81.24564330381325\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[0], 'LIMS')


    def test_parse_adt_a01(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240107133000||ADT^A01|||2.5\r"
            "PID|1||185620675||KAYLA HENRY||20211106|F\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['mrn'], '185620675')
        self.assertEqual(parsed_message[1]['dob'], '2021-11-06 00:00:00')
        self.assertEqual(parsed_message[1]['sex'], 1)


    def test_parse_adt_a03(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331054700||ADT^A03|||2.5\r"
            "PID|1||112034143\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['mrn'], '112034143')


    def test_parse_oru_r01_with_seconds(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331005400||ORU^R01|||2.5\r"
            "PID|1||157828764\r"
            "OBR|1||||||20240331005400\r"
            "OBX|1|SN|CREATININE||81.24564330381325\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['mrn'], '157828764')
        self.assertEqual(parsed_message[1]['results'][0]['date'], '2024-03-31 00:54:00')
        self.assertEqual(parsed_message[1]['results'][0]['result'], '81.24564330381325')


    def test_parse_oru_r01_with_minutes(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||202401201630||ORU^R01|||2.5\r"
            "PID|1||478237423\r"
            "OBR|1||||||202401202243\r"
            "OBX|1|SN|CREATININE||103.4\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['results'][0]['date'], '2024-01-20 22:43:00')


    def test_parse_oru_r01_with_hours(self):
        message = (
        "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331073300||ORU^R01|||2.5\r"
        "PID|1||172480767\r"
        "OBR|1||||||2024033107\r"
        "OBX|1|SN|CREATININE||55.459808442525905\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['results'][0]['date'],  '2024-03-31 07:00:00')


    def test_parse_oru_r01_no_time(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331073300||ORU^R01|||2.5\r"
            "PID|1||172480767\r"
            "OBR|1||||||20240331\r"
            "OBX|1|SN|CREATININE||55.459808442525905\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['results'][0]['date'], '2024-03-31 00:00:00')


    def test_adt_a01_missing_dob(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240107133000||ADT^A01|||2.5\r"
            "PID|1||185620675||KAYLA HENRY|||F\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['mrn'], '185620675')
        self.assertIsNone(parsed_message[1]['dob'])
        self.assertEqual(parsed_message[1]['sex'], 1)


    def test_adt_a01_missing_sex(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240107133000||ADT^A01|||2.5\r"
            "PID|1||185620675||KAYLA HENRY||20211106|\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['mrn'], '185620675')
        self.assertEqual(parsed_message[1]['dob'], '2021-11-06 00:00:00')
        self.assertIsNone(parsed_message[1]['sex'])


    def test_parse_oru_r01_missing_date(self):
        message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331073300||ORU^R01|||2.5\r"
            "PID|1||172480767\r"
            "OBR|1||||||\r"
            "OBX|1|SN|CREATININE||55.459808442525905\r"
        )
        parsed_message = self.parser.parse(message)
        self.assertEqual(parsed_message[1]['results'][0]['date'], ' ')


if __name__ == "__main__":
    unittest.main()
