from DigitalWallet.UserFactory.user_factory import UserFactory
from datetime import datetime
from uuid import uuid4

class UserManager:
    def __init__(self, factory : UserFactory):
        self.__factory = factory

    def make_user(self,name):
        id = uuid4()
        time = datetime.now()
        return self.__factory.make_user(id, name, time)