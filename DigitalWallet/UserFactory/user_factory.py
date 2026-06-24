from abc import ABC, abstractmethod
from DigitalWallet.User.user import User

class UserFactory(ABC):
    @abstractmethod
    def make_user(self, id, name, created_at):
        pass

class NormalUser(UserFactory):
    def make_user(self, id, name, created_at):
        return User(id, name, created_at)
    
class PrivilegdedUser(UserFactory):
    pass