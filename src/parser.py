from hl7apy.parser import parse_message
from datetime import datetime


class HL7MessageParser:
    def parse(self, hl7_message):
        """Determines the message type and routes to the appropriate handler."""
        message = parse_message(hl7_message, find_groups=False)
        pid = message.PID
        mrn = pid.PID_3.value
        msg_type = message.msh.MSH_9.value

        if msg_type == "ADT^A01":
            return self._handle_adt_a01(pid, mrn)
        elif msg_type == "ADT^A03":
            return self._handle_adt_a03(mrn)
        elif msg_type == "ORU^R01":
            return self._handle_oru_r01(message, mrn)
        else:
            return "OTHER"

    def _handle_adt_a01(self, pid, mrn):
        """Handles ADT^A01 (Patient Admission) messages."""
        dob = self._convert_to_datetime(pid.PID_7.value)
        sex = pid.PID_8.value
        sex = 1 if sex == "F" else 0
        mrn = pid.PID_3.value
        return "PAS_admit", {"mrn": mrn, "dob": dob, "sex": sex}

    def _handle_adt_a03(self, mrn):
        """Handles ADT^A03 (Patient Discharge) messages."""
        return "PAS_discharge", {"mrn": mrn}

    def _handle_oru_r01(self, message, mrn):
        """Handles ORU^R01 (Lab Results) messages."""
        results = []

        current_obr = None  # current message segment

        for segment in message.children:  
            if segment.name == "OBR":
                current_obr = segment  
            
            elif segment.name == "OBX" and segment.OBX_3.value == "CREATININE":
                creatinine_value = segment.OBX_5.value 

                creatinine_date = self._convert_to_datetime(current_obr.OBR_7.value) if current_obr.OBR_7.value else " "

                results.append({
                    "result": creatinine_value,
                    "date": creatinine_date
                })

        if not results:
            return "OTHER"  # No Creatinine test found

        return "LIMS", {"mrn": mrn, "results": results}

    @staticmethod
    def _convert_to_datetime(date_str):
        """Converts HL7 date string (YYYYMMDD, YYYYMMDDHHMM, YYYYMMDDHHMMSS) to a formatted datetime string (YYYY-MM-DD HH:MM:SS)."""
        if not date_str:
            return None
        try:
            if len(date_str) == 8:
                dt = datetime.strptime(date_str, "%Y%m%d")
            elif len(date_str) == 10:
                dt = datetime.strptime(date_str, "%Y%m%d%H")
            elif len(date_str) == 12:
                dt = datetime.strptime(date_str, "%Y%m%d%H%M")
            elif len(date_str) == 14:
                dt = datetime.strptime(date_str, "%Y%m%d%H%M%S")
            else:
                return None
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


if __name__ == "__main__":
    # Example HL7 message
    message1 = (
        "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||202401201630||ORU^R01|||2.5\r"
        "PID|1||478237423\r"
        "OBR|1||||||202401202243\r"
        "OBX|1|SN|CREATININE||103.4\r"
        "OBR|1||||||202401202243\r"
        "OBX|1|SN|CREATININE||100.4\r"
    )

    message2 = (
        "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240107133000||ADT^A01|||2.5\r"
        "PID|1||185620675||KAYLA HENRY||20211106|F\r"
    )

    message3 = (
        "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331054700||ADT^A03|||2.5\r"
        "PID|1||112034143\r"
    )

    message4 = (
        "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240331005400||ORU^R01|||2.5\r"
        "PID|1||157828764\r"
        "OBR|1||||||20240331005400\r"
        "OBX|1|SN|CREATININE||81.24564330381325\r"
    )
    parser = HL7MessageParser()
    parsed_message = parser.parse(message4)
    print(parsed_message)
    # print(parser.parse())
