from enum import Enum

class IntentStatus(Enum):
    INITIALISED = "INTIALISED"
    FAILED = "FAILED"
    PROCESSED = "PROCESSED"