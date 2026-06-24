from typing import Optional
from DigitalWallet.Errors.errors import InSufficientFunds
from threading import Lock

class Account:
    def __init__(self, id, owner_id):
        self.__id = id
        self.__owner_id = owner_id
        # self.__type = type
        self.__balance = 0.0
        self.__transaction_history = []

    @property
    def balance(self):
        return self.__balance
    
    @property
    def id(self):
        return self.__id

    @property
    def owner(self):
        return self.__owner_id
    
    def debit(self, amount, current_time):
        if amount < self.__balance:
            old_amount = self.__balance
            new_amount = self.__balance - amount
            self.__balance = new_amount
            self.__transaction_history.append((current_time, old_amount, new_amount, "DEBIT"))
        else:
            raise InSufficientFunds()
    
    def credit(self, amount, current_time):
        old_amount = self.__balance
        new_amount = self.__balance + amount
        self.__balance = new_amount
        self.__transaction_history.append((current_time, old_amount, new_amount, "CREDIT"))

        