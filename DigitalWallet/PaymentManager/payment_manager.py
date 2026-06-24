from DigitalWallet.Utility.Intent.intent import Intent
from DigitalWallet.Utility.Intent.intent_status import IntentStatus

class PaymentManager:
    def __init__(self):
        pass

    def __create_intent(self, user_a, user_b, amount):
        return Intent(user_a, user_b, amount)
    
    def handle(self, user_a, user_b, amount):
        intent = self.__create_intent(user_a, user_b, amount)
        return intent

    def change_status(self, intent : Intent, new_status):
        intent.change_status(new_status)


