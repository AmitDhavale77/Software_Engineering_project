from hl7apy.parser import parse_message
from datetime import datetime

class HL7MessageParser:
    def __init__(self, hl7_message):
        """
        Initializes the HL7MessageParser with the raw HL7 message.
        """
        self.message = parse_message(hl7_message, find_groups=False)
        self.msh = self.message.MSH
        self.pid = self.message.PID
        self.mrn = self.pid.PID_3.value
        self.msg_type = self.msh.MSH_9.value

    def parse(self):
        """
        Determines the message type and routes to the appropriate handler.
        """
        if self.msg_type == 'ADT^A01':
            return self._handle_adt_a01()
        elif self.msg_type == 'ADT^A03':
            return self._handle_adt_a03()
        elif self.msg_type == 'ORU^R01':
            return self._handle_oru_r01()
        else:
            return {'message': 'OTHER'}

    def _handle_adt_a01(self):
        """
        Handles ADT^A01 (Patient Admission) messages.
        """
        dob = self._convert_to_datetime(self.pid.PID_7.value)
        sex = self.pid.PID_8.value
        return {'message': 'PAS_admit', 'mrn': self.mrn, 'DOB': dob, 'sex': sex}

    def _handle_adt_a03(self):
        """
        Handles ADT^A03 (Patient Discharge) messages.
        """
        return {'message': 'PAS_discharge', 'mrn': self.mrn}

    def _handle_oru_r01(self):
        """
        Handles ORU^R01 (Lab Results) messages.
        """
        obr = self.message.OBR
        obx = self.message.OBX


        # Validate if the result is for creatinine
        if obx.OBX_3.value != 'CREATININE':
            return {'message': 'OTHER'}

        creatinine_date = self._convert_to_datetime(obr.OBR_7.value)
        creatinine_level = obx.OBX_5.value

        return {
            'message': 'LIMS',
            'mrn': self.mrn,
            'date': creatinine_date,
            'result': creatinine_level
        }

    @staticmethod
    def _convert_to_datetime(date_str):
        """
        Converts HL7 date string (YYYYMMDD or YYYYMMDDHHMM) to a formatted datetime string (YYYY-MM-DD HH:MM:SS).
        """
        if not date_str:
            return None
        try:
            if len(date_str) == 8:
                dt = datetime.strptime(date_str, '%Y%m%d')
            elif len(date_str) == 12:
                dt = datetime.strptime(date_str, '%Y%m%d%H%M')
            else:
                return None
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None


if __name__ == "__main__":
    # Example HL7 message
    message = (
        "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||202401201630||ORU^R01|||2.5\r"
        "PID|1||478237423\r"
        "OBR|1||||||202401202243\r"
        "OBX|1|SN|CREATININE||103.4\r"
    )

    parser = HL7MessageParser(message)
    print(parser.parse())
