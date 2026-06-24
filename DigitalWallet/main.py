from DigitalWallet.Orchestrator.orchestrator import Orchestrator
from DigitalWallet.AccountManager.account_manager import AccountManager
from DigitalWallet.PaymentManager.payment_manager import PaymentManager
from DigitalWallet.UserManager.user_manager import UserManager
from DigitalWallet.UserFactory.user_factory import NormalUser

factory = NormalUser()

paymentManager = PaymentManager()
userManager = UserManager(factory)
accountManager = AccountManager()
manager = Orchestrator(paymentManager, accountManager, userManager)

userA = userManager.make_user("Ayush")
userB = userManager.make_user("Richa")

userAAccount = accountManager.create_account(userA.id)
userBAccount = accountManager.create_account(userB.id)

accountManager.add_balance(userAAccount.id, 100.0)
accountManager.add_balance(userBAccount.id, 1000.0)

print(accountManager.check_balance(userAAccount.id))
print(accountManager.check_balance(userBAccount.id))

manager.transfer(userBAccount.id, userAAccount.id, 100.0)
manager.transfer(userAAccount.id, userBAccount.id, 100.0)

print(accountManager.check_balance(userAAccount.id))
print(accountManager.check_balance(userBAccount.id))