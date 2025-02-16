import simulator
from datetime import datetime


def to_mllp(segments):
    m = bytes(chr(simulator.MLLP_START_OF_BLOCK), "ascii")
    m += bytes("\r".join(segments) + "\r", "ascii")
    m += bytes(
        chr(simulator.MLLP_END_OF_BLOCK) + chr(simulator.MLLP_CARRIAGE_RETURN), "ascii"
    )
    return m


def create_acknowledgement(ack_type):
    assert ack_type in ["AA", "AE", "AR"]
    ack = [
        fr"MSH|^~\&|||||{datetime.now().strftime("%Y%m%d%H%M%S")}||ACK|||2.5",
        fr"MSA|{ack_type}",
    ]
    return to_mllp(ack)
