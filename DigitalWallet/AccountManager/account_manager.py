from DigitalWallet.Utility.Intent.intent import Intent
from DigitalWallet.Utility.Transfer.transfer_type import TranferType
from DigitalWallet.Account.account import Account
from threading import Lock
from uuid import uuid4
from datetime import datetime

class AccountManager:
    def __init__(self):
        self.__accounts = {}
        self.__owner_to_accounts : dict = {}

    def handle(self, intent : Intent):
        time = intent.created_at
        from_user = intent.from_user
        to_user = intent.to_user
        self.__accounts[from_user].debit(intent.amount, time)
        self.__accounts[to_user].credit(intent.amount, time)
            
    def create_account(self, owner_id):
        id = uuid4()
        new_account = Account(id, owner_id)
        self.__accounts[id] = new_account
        if owner_id not in self.__owner_to_accounts:
            self.__owner_to_accounts[owner_id] = []
        self.__owner_to_accounts[owner_id].append(id)
        return new_account
    
    def check_balance(self,account_id):
        return self.__accounts[account_id].balance
    
    def add_balance(self, account_id, amount):
        current_time = datetime.now()
        self.__accounts[account_id].credit(amount, current_time)

