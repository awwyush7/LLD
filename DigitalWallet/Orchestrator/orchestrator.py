from DigitalWallet.Utility.Intent.intent_status import IntentStatus
from datetime import datetime

class Orchestrator:
    def __init__(self, payment_manager, account_manager, user_manager):
        self.__payment_manager = payment_manager
        self.__account_manager = account_manager
        self.__user_manager = user_manager
        

    def transfer(self, user_a, user_b, amount):
        intent = self.__payment_manager.handle(user_a, user_b, amount)
        try:
            self.__account_manager.handle(intent)
            self.__payment_manager.change_status(intent, IntentStatus.PROCESSED)
        except Exception as e:
            self.__payment_manager.change_status(intent, IntentStatus.FAILED)
    