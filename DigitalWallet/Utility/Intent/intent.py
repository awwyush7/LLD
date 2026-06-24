from DigitalWallet.Utility.Intent.intent_status import IntentStatus
from datetime import datetime

class Intent:
    def __init__(self, from_user, to_user, amount):
        self.__from_user = from_user
        self.__to_user = to_user
        self.__amount = amount
        self.__status = IntentStatus.INITIALISED
        self.__created_at = datetime.now()
    
    @property
    def created_at(self):
        return self.__created_at

    @property
    def amount(self):
        return self.__amount
    
    @property
    def from_user(self):
        return self.__from_user
    @property
    def to_user(self):
        return self.__to_user
    
    def change_status(self, new_status : IntentStatus):
        self.__status == new_status